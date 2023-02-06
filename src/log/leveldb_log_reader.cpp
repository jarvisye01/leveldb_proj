// c library
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

// c++ library
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <string>
#include <utility>

/*
 * A tool to parse leveldb's log file.
 * eg: leveldb_log_reader xxxx.log
 * This tool will print all record in log.
 * 'Put(key, val)' for kTypeValue
 * 'Delete(key)' for kTypeDeletion
 */

enum CHUNK_TYPE: unsigned int
{
    kZeroType = 0,
    kFullType = 1,
    kFirstType = 2,
    kMiddleType = 3,
    kLastType = 4
};

enum RECORD_TYPE: unsigned int
{
    kTypeDeletion = 0,
    kTypeValue = 1
};

uint32_t DecodeFixed32(const char * ptr)
{
    const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);

    // Recent clang and gcc optimize this to a single mov / ldr instruction.
    return (static_cast<uint32_t>(buffer[0])) |
        (static_cast<uint32_t>(buffer[1]) << 8) |
        (static_cast<uint32_t>(buffer[2]) << 16) |
        (static_cast<uint32_t>(buffer[3]) << 24);
}

uint64_t DecodeFixed64(const char * ptr)
{
    const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);

    // Recent clang and gcc optimize this to a single mov / ldr instruction.
    return (static_cast<uint64_t>(buffer[0])) |
        (static_cast<uint64_t>(buffer[1]) << 8) |
        (static_cast<uint64_t>(buffer[2]) << 16) |
        (static_cast<uint64_t>(buffer[3]) << 24) |
        (static_cast<uint64_t>(buffer[4]) << 32) |
        (static_cast<uint64_t>(buffer[5]) << 40) |
        (static_cast<uint64_t>(buffer[6]) << 48) |
        (static_cast<uint64_t>(buffer[7]) << 56);
}

uint32_t GetVarint32(const char * ptr, size_t & mov)
{
    uint32_t result = 0;
    mov = 0;
    for (uint32_t shift = 0; shift <= 28; shift += 7) {
        uint32_t byte = *(reinterpret_cast<const uint8_t*>(ptr));
        ptr++;
        mov++;
        if (byte & 128) {
            // More bytes are present
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            return result;
        }
    }
    return result;
}

uint64_t GetVarint64(const char * ptr, size_t & mov)
{
    uint64_t result = 0;
    mov = 0;
    for (uint32_t shift = 0; shift <= 28; shift += 7) {
        uint64_t byte = *(reinterpret_cast<const uint8_t*>(ptr));
        ptr++;
        mov++;
        if (byte & 128) {
            // More bytes are present
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            return result;
        }
    }
    return result;
}

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
constexpr const int kHeaderSize = 4 + 2 + 1;

constexpr const int kBlockSize = 1024 * 32;

class LogReader
{
public:
    LogReader(const std::string & log_file_name) noexcept;
    ~LogReader() noexcept;
    LogReader(const LogReader &) = delete;
    LogReader(const LogReader &&) = delete;
    LogReader& operator=(const LogReader &) = delete;
    LogReader& operator=(const LogReader &&) = delete;

    bool Good() const;
    bool HasNext() const;
    int Next(std::string & next);
private:
    static int Read(int fd, size_t len, char * buf);
    static const char* GetChunk(const char * buf, std::string & data, CHUNK_TYPE & type);

    std::string log_file_name_;
    bool is_good_{false};
    int fd_{-1};
    size_t file_size_{0};

    char buf_[kBlockSize]{0};
    size_t r_pos_{0};
    bool has_read_all_block_{true};
    size_t real_block_size_{0};
    size_t has_read_block_size_{0};
};

int LogReader::Read(int fd, size_t len, char * buf)
{
    int read_size = ::read(fd, buf, len);
    if (read_size < 0)
        return -1;
    return read_size;
}

const char* LogReader::GetChunk(const char * buf, std::string & data, CHUNK_TYPE & type)
{
    data.clear();
    const uint32_t a = static_cast<uint32_t>(buf[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(buf[5]) & 0xff;
    type = static_cast<CHUNK_TYPE>(buf[6]);
    const uint32_t data_len = a | (b << 8);
    data.append(buf + kHeaderSize, data_len);
    return buf + kHeaderSize + data_len;
}

LogReader::LogReader(const std::string & log_file_name) noexcept: log_file_name_(log_file_name)
{
    fd_ = ::open(log_file_name_.c_str(), O_CREAT | O_RDONLY);
    if (fd_ > 0)
        is_good_ = true;
    else
        return;

    file_size_ = ::lseek(fd_, 0, SEEK_END);
    lseek(fd_, 0, SEEK_SET);
}

LogReader::~LogReader() noexcept
{
    if (Good())
        ::close(fd_);
}

bool LogReader::Good() const
{
    return is_good_;
}

bool LogReader::HasNext() const
{
    return r_pos_ < file_size_ || !has_read_all_block_;
}

int LogReader::Next(std::string & next)
{
    int ret = 0;
    next.clear();
    while (true && HasNext())
    {
        if (has_read_all_block_)
        {
            memset(buf_, 0, kBlockSize);
            ret = Read(fd_, kBlockSize, buf_);
            if (ret < 0)
            {
                is_good_ = false;
                return ret;
            }
            r_pos_ += ret;
            has_read_all_block_ = false;
            has_read_block_size_ = 0;
            real_block_size_ = ret;
        }

        std::string chunk_data;
        CHUNK_TYPE type = CHUNK_TYPE::kZeroType;
        const char * buf = buf_ + has_read_block_size_;
        std::string tmp_chunk;
        buf = GetChunk(buf, chunk_data, type);

        if (chunk_data.size() > 0)
            next.append(chunk_data);

        has_read_block_size_ += kHeaderSize + chunk_data.size();

        if (has_read_block_size_ == kBlockSize || has_read_block_size_ + kHeaderSize > real_block_size_)
        {
            has_read_all_block_ = true;
            r_pos_ += real_block_size_ - has_read_block_size_;
        }

        if (type == CHUNK_TYPE::kFullType || type == CHUNK_TYPE::kLastType)
            break;
    }
    return 0;
}


int main(int argc, char ** argv)
{
    if (argc < 2)
    {
        printf("ERR: Invalid params \"leveldb_log_reader <log_file_name>\"\n");
        return 0;
    }
    std::string log_file_name(argv[1]);
    LogReader reader(log_file_name);
    if (!reader.Good())
    {
        printf("ERR: Reader is not good\n");
        return 0;
    }

    while (reader.Good() && reader.HasNext())
    {
        std::string record;
        int ret = reader.Next(record);
        if (ret != 0)
        {
            printf("ERR: Read error\n");
            return 0;
        }

        const char * start = record.c_str();
        size_t read_size = 0;
        while (read_size < record.size())
        {
            uint64_t seq_num = DecodeFixed64(start + read_size);
            read_size += sizeof(uint64_t);
            uint32_t entry_count = DecodeFixed32(start + read_size);
            read_size += sizeof(uint32_t);

            // parse entry
            for (size_t i = 0; i < entry_count; i++)
            {
                RECORD_TYPE r_type = static_cast<RECORD_TYPE>(start[read_size]);
                read_size += 1;

                std::string key, val;
                uint32_t key_len = 0, val_len = 0;
                size_t mov = 0;
                if (r_type == RECORD_TYPE::kTypeValue)
                {
                    key_len = GetVarint32(start + read_size, mov);
                    read_size += mov;
                    key = std::string(start + read_size, key_len);
                    read_size += key_len;

                    val_len = GetVarint32(start + read_size, mov);
                    read_size += mov;
                    val = std::string(start + read_size, val_len);
                    read_size += val_len;

                    printf("Put(%s, %s) seq_num %zu\n", key.c_str(), val.c_str(), seq_num++);
                }
                else if (r_type == RECORD_TYPE::kTypeDeletion)
                {
                    key_len = GetVarint32(start + read_size, mov);
                    read_size += mov;
                    key = std::string(start + read_size, key_len);
                    read_size += key_len;
                    printf("Delete(%s) seq_num %zu\n", key.c_str(), seq_num++);
                }
                else
                {
                    printf("ERR: Parse record error\n");
                    return 0;
                }
            }
        }
    }
    return 0;
}
