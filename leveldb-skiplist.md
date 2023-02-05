# leveldb源码阅读系列之skiplist(2023.02.05)

### 1.background

当前工作使用的主要存储引擎就是leveldb，相比于传统基于的B tree存储引擎，基于LSM tree存储引擎的db在高并发、SSD存储介质的场景下拥有更好的写入性能，leveldb更是LSM tree类存储引擎的代表作。

本身工作使用MySQL较少，所以也很难去深入的研究，这里就好好研究一下leveldb相关的东西，写一个源码分析的系列，好好沉淀一下自己。

稍微看过leveldb架构的同学都知道leveldb内存的memory table是基于SkipList来实现的，那么今天就好好来看一下SkipList。这里之前其实也自己实现过skiplist，发现性能不是特别好，来这里找一下差距。

### 2.源码分析

本以为实现会比较复杂，看过之后发现leveldb的SkipList实现的相当的简洁，400行代码不到，SkipList中最繁琐的Insert操作非常简洁优雅。真实的实现是带模板的，我介绍的时候会忽略模板，重点关注相关数据结构和算法。

**a.数据结构**

SkipList数据结构主要包含两个东西，存储节点Node和SkipList本身：

```c++
template <typename Key, class Comparator>                                                                   
struct SkipList<Key, Comparator>::Node {
    // ...
    Key const key;
private:
    std::atomic<Node*> next_[1];
};

template <typename Key, class Comparator>
class SkipList {
	// ...
private:
	Node* const head_;
    // Modified only by Insert().  Read racily by readers, but stale
    // values are ok.
	std::atomic<int> max_height_;  // Height of the entire list
};

```

其中Node节点只存储Key和一个多层的Next数组，SkipList只存储一个头节点Node和最大高度。你可能注意到一个问题，这里的Node甚至都不是双向的，因此整个SkipList不能从后向前遍历，因为memory table就没有从后向前遍历的需求。普遍情况下我们会把SkipList中的Node设计成一个双向的，leveldb没有遵守这一点却反而让自身更加简洁，值得我们借鉴。另外再提一点，leveldb的SkipList甚至没有实现Delete操作（同样是不需要），使得代码进一步简洁。

**b.相关方法**

对于SkipList，个人认为最重要的就是一个search方法，search去查找一个key，并且返回一个前置指针的数组，后续的Insert和Delete都会依赖这个操作。

在leveldb的实现中，这个实现叫做**FindGreaterOrEqual**，代码也很简单：

```c++
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key, Node** prev) const {
    Node* x = head_;
    int level = GetMaxHeight() - 1;
    while (true) {
        Node* next = x->Next(level);
        if (KeyIsAfterNode(key, next)) {
            // 如果next节点的值要小于key，则去到next节点继续查找
            // Keep searching in this list
            x = next;
        } else {
            // 如果next节点的值大于或等于key，则需要去下一个level搜索，并且记录当前level的前置指针
            if (prev != nullptr) prev[level] = x;
            if (level == 0) {
                // level为0说明到了第一层，返回next指针
                return next;
            } else {
                // Switch to next list
                level--;
            }
        }
    }
}
```

有了这个**FindGreaterOrEqual**之后实现Insert和Delete都很简单，Insert在leveldb源码中已经有实现了：

```c++
template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key& key) {
    Node* prev[kMaxHeight];
    Node* x = FindGreaterOrEqual(key, prev);

    // leveldb当前不允许插入重复的key
    assert(x == nullptr || !Equal(key, x->key));

    int height = RandomHeight();
    // 新节点的高度超过了max_height_，则head_节点需要指向这些超过的部分
    if (height > GetMaxHeight()) {
        for (int i = GetMaxHeight(); i < height; i++) {
            prev[i] = head_;
        }
        // It is ok to mutate max_height_ without any synchronization
        // with concurrent readers.  A concurrent reader that observes
        // the new value of max_height_ will see either the old value of
        // new level pointers from head_ (nullptr), or a new value set in
        // the loop below.  In the former case the reader will
        // immediately drop to the next level since nullptr sorts after all
        // keys.  In the latter case the reader will use the new node.
        max_height_.store(height, std::memory_order_relaxed);
    }

    x = NewNode(key, height);
    for (int i = 0; i < height; i++) {
        // 修改prev数组中的节点指向新节点，新节点指向prev数组元素原来指向的节点
        // NoBarrier_SetNext() suffices since we will add a barrier when
        // we publish a pointer to "x" in prev[i].
        x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
        prev[i]->SetNext(i, x);
    }
}
```

对于Delete，leveldb并无实现，其实也很简单，找到指定的Node之后，将prev数组中的指针指向该Node的next指针即可，可以看下[这个实现](!https://github.com/Mr-xingJian/algorithm/blob/main/skiplist/skiplist_v2.cpp)。

**c.层高的计算**

一个SkipList性能的好坏很重要的就是整个跳表结构是否优良，很重要的就是插入一个节点的时候如何确定这个节点高度，这里牵扯到了很多数学问题，就不去深究理论了，直接来看leveldb是如何实现的。

leveldb对于整个跳表的高度做了一个硬性限制，kMaxHeight=12，最大层级不能超过12。对于插入节点的高度是这么计算的：

```c++
int height = 1;
// kBranching = 4
// rnd_.OneIn(kBranching)有1/4的概率返回true
while (height < kMaxHeight && rnd_.OneIn(kBranching)) {
    height++;
}
```

通过代码不难看出层高为k的概率大概是(1/4)^k * 3/4，具体可以google一下SkipList层高计算对性能影响相关的资料，平时项目如果有需求亦可以直接借鉴这个实现。

### 3.总结

leveldb的代码确实极其优雅，非常值得我们学习。不过leveldb的SkipList为了实现memory table的功能，会有一些额外的方法，如果仅仅是想要看SkipList的实现可以看一下[这里的代码](!https://github.com/Mr-xingJian/algorithm/blob/main/skiplist/skiplist_v2.cpp)，整体的逻辑与leveldb的SkipList基本一致，但是更加简单，这个代码可以通过leetcode中实现跳表那一题，不过leetcode中的key已经指定为int了，你可以这么做来进行适配：

```c++
// SkipList<Key>

class Skiplist {
public:
    Skiplist() {}
    
    bool search(int target) {
        return slist.Contains(target);
    }
    
    void add(int num) {
        slist.Insert(num);
    }
    
    bool erase(int num) {
        return slist.Delete(num);
    }
private:
    SkipList<int> slist;
};
```

看了SkipList的实现，leveldb的memory table是实现也就比较简单了，跟上面的代码很相似，都是在SkipList的基础上再封装一层即可。

