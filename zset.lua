
--[[
    author:zhouqili
    time:2019-05-14 21:39:01

    根据redis里的skipList实现的跳表模块, 用于排行榜的数据结构。

    例子：

    local tbRankBoard = skiplist:new()  -- 如果需要自定义排序比较, 需要传入比较方法, 默认方法是本文件内的defaultComFunc。
                                           比较方法中value相同return 0, 排前面排前面return 1, 否则 -1。
                                           相同value会默认按照插入榜中的时间顺序排序。

    tbRankBoard:add(1,"key")        -- 写入数据, value可以是结构体
    tbRankBoard:getAll(true)         -- 获取所有排名数据, 参数字段的作用是是否返回value, 传true时返回一个{{key = key, value = value}}结构的数组, 不传或传false时仅返回由key组成的数组
    tbRankBoard:getByKey("key")      -- 通过key获取对应的value
    tbRankBoard:getByRank(1)         -- 获取对应名次的key和value
    tbRankBoard:rank("key")          -- 获取key对应的rank排名
    tbRankBoard:rem("key")           -- 删除元素
]]



local SKIPLIST_MAXLEVEL = 32                    -- 最大32层
local SKIPLIST_P        = 0.25                  -- redis源码里是0.25, 跟着写的
--
local skipList_meta     = {}
skipList_meta.__index   = skipList_meta
--
local zset_meta         = {}
zset_meta.__index       = zset_meta
local zset              = setmetatable({}, zset_meta)
--------------------------------------------------------------

local function randomLevel()
    local level = 1
    while(math.random(1, 0xffff) < SKIPLIST_P * 0xffff) do
        level = level + 1
    end
    return level < SKIPLIST_MAXLEVEL and level or SKIPLIST_MAXLEVEL
end

local function createSkipListNode(level, key, value)
    local sln = { key = key, value = value, level = {}, backward = nil}
    for lv = 1, level do
        table.insert(sln.level, {forward = nil, span = 0})
    end
    return sln
end

local function createSkipList(cmpFn)
    assert(type(cmpFn) == "function")
    return setmetatable({
        header     = createSkipListNode(SKIPLIST_MAXLEVEL),
        tail       = nil,
        length     = 0,
        level      = 1,
        compareFn  = cmpFn,
    }, skipList_meta)
end

---------------------------skipList---------------------------

--[[
    @desc:          数据插入
    --@key:
	--@value:
    @return:
]]
function skipList_meta:insert(key, value)
    local update = {}
    local rank   = {}
    local x      = self.header
    local level
    for i = self.level, 1, -1 do
        -- 找到所有level中的节点位置
        rank[i] = i == self.level and 0 or rank[i+1]
        while x.level[i].forward and (self.compareFn(x.level[i].forward.value, value) < 0 or self.compareFn(x.level[i].forward.value, value) == 0 and x.level[i].forward.key ~= key )do
            rank[i] = rank[i] + x.level[i].span
            x = x.level[i].forward
        end
        update[i] = x
    end

    -- 允许修改分数, 所以在insert前需要判断key值是否已经在objs中。这里就不判断了, 默认当作key值不存在做插入处理。
    level = randomLevel()
    if level > self.level then
        for i = self.level + 1, level do
            rank[i] = 0
            update[i] = self.header
            update[i].level[i].span = self.length
        end
        self.level = level
    end
    x = createSkipListNode(level, key, value)
    for i = 1, level do
        x.level[i].forward = update[i].level[i].forward
        update[i].level[i].forward = x

        x.level[i].span = update[i].level[i].span - (rank[1] - rank[i])
        update[i].level[i].span = (rank[1] - rank[i]) + 1
    end

    for i = level + 1, self.level do
        update[i].level[i].span = update[i].level[i].span + 1
    end

    x.backward = update[1] ~= self.header and update[1] or nil
    if x.level[1].forward then
        x.level[1].forward.backward = x
    else
        self.tail = x
    end
    self.length = self.length + 1
end

function skipList_meta:getRank(key, value)
    local rank = 0
    local x
    x = self.header
    for i = self.level, 1, -1 do
        while x.level[i].forward and (self.compareFn(x.level[i].forward.value, value) < 0 or (self.compareFn(x.level[i].forward.value, value) == 0 and x.level[i].forward.key ~= key)) do
            rank = rank + x.level[i].span
            x = x.level[i].forward
        end
        if x.level[i].forward and x.level[i].forward.value and x.level[i].forward.key == key and x.level[i].forward.value == value then  
            rank = rank + x.level[i].span
            return rank
        end
    end
    return 0
end

--[[
    @desc:          获取对应排名的元素
    --@rank:
    @return:
]]
function skipList_meta:getNodeByRank(rank)
    if rank <= 0 or rank > self.length then
        return
    end
    local traversed = 0
    local x = self.header
    for i = self.level, 1, -1 do
        while x.level[i].forward and traversed + x.level[i].span <= rank do
            traversed = traversed + x.level[i].span
            x = x.level[i].forward
        end
        if traversed == rank then
            return x
        end
    end
end

--[[
    @desc:          delete使用的内部函数
    --@node:
	--@update:
    @return:
]]
function skipList_meta:deleteNode(node, update)
    for i = 1, self.level do
        if update[i].level[i].forward == node then
            update[i].level[i].span = update[i].level[i].span + node.level[i].span - 1
            update[i].level[i].forward = node.level[i].forward
        else
            update[i].level[i].span = update[i].level[i].span - 1
        end
    end
    if node.level[1].forward then
        node.level[1].forward.backward = node.backward
    else
        self.tail = node.backward
    end
    while self.level > 2 and not self.header.level[self.level -1].forward do
        self.level = self.level - 1
    end
    self.length = self.length - 1
end

--[[
    @desc:          删除
    --@key:
	--@value:
    @return:
]]
function skipList_meta:delete(key, value)
    local update = {}
    local x = self.header
    for i = self.level, 1, -1 do
        while x.level[i].forward and (self.compareFn(x.level[i].forward.value, value) < 0 or (self.compareFn(x.level[i].forward.value, value) == 0 and x.level[i].forward.key ~= key)) do
            x = x.level[i].forward
        end
        update[i] = x
    end
    -- 确保对象的正确
    x = x.level[1].forward
    if x and x.key == key and x.value == value then
        self:deleteNode(x, update)
        return true
    end
    return false
end

function skipList_meta:rank_range(start, stop)
    local reverse, rangelen
    if start <= stop then
        reverse = 0
        rangelen = stop - start + 1
    else
        reverse = 1
        rangelen = start - stop + 1
    end

    local node = self:getNodeByRank(start)
    local result = {}
    local n = 0
    while node and n < rangelen do
        n = n + 1
        result[#result+1] = node.key
        node = (reverse == 1) and node.backward or node.level[1].forward
    end
    return result
end

function skipList_meta:get_count()
    return self.length
end

function skipList_meta:reverse_rank(r)
    return self.length - r + 1
end

function skipList_meta:isInRange(min,max)
    if min > max then
        return 0
    end
    local x = self.tail
    if x == nil or x.value < min then
        return 0
    end
    x = self.header.level[1].forward
    if x == nil or x.value > max then
        return 0
    end
    return 1
end

function skipList_meta:firstInRange(min,max)
    -- If everything is out of range, return early.
    if not self:isInRange(min, max) then
        return nil
    end

    local x = self.header
    for i = self.level, 1, -1 do
        -- Go forward while *OUT* of range.
        while x.level[i].forward and x.level[i].forward.value < min do
            x = x.level[i].forward
        end
    end

    -- This is an inner range, so the next node cannot be NULL.
    x = x.level[1].forward
    return x
end

--[[ 
Find the last node that is contained in the specified range.
Returns NULL when no element is contained in the range.
]]
function skipList_meta:lastInRange(min,max)
    -- If everything is out of range, return early.
    if not self:isInRange(min, max) then
        return nil
    end

    local x = self.header
    for i = self.level, 1, -1 do
        -- Go forward while *OUT* of range.
        while x.level[i].forward and x.level[i].forward.value <= max do
            x = x.level[i].forward
        end
    end
    -- This is an inner range, so this node cannot be NULL.
    return x
end

function skipList_meta:get_score_range(s1,s2)
    local reverse,node
    if s1 <= s2 then
        reverse = 0
        node = self:firstInRange(s1, s2)
    else
        reverse = 1
        node = self:lastInRange(s2, s1)
    end
    local ret = {}
    while node ~= nil do
        if reverse == 1 then
            if node.value < s2 then
                break
            end
        else
            if node.value > s2 then
                break
            end
        end
        table.insert(ret,node.key)
        node = (reverse == 1) and node.backward or node.level[1].forward
    end
    return ret
end

--[[
    Delete all the elements with rank between start and end from the skiplist.
    Start and end are inclusive. Note that start and end need to be 1-based
]]
function skipList_meta:deleteByRank(start,stop,cb)
    if start > stop then
        local tmp = start
        start = stop
        stop = tmp
    end
    local update = {}
    local x = self.header
    local traversed,removed = 0,0
    for i = self.level,1, -1 do
        while x.level[i].forward and (traversed + x.level[i].span) < start do
            traversed = traversed + x.level[i].span
            x = x.level[i].forward
        end
        update[i] = x
    end

    traversed = traversed + 1
    x = x.level[1].forward
    while x and traversed <= stop do
        local next_node = x.level[1].forward
        self:deleteNode(x,update)
        cb(x.key)
        removed = removed + 1
        traversed = traversed + 1
        x = next_node
    end
    return removed
end

function skipList_meta:dump()
    local x = self.header
    local i = 0
    while x.level[1].forward do
        x = x.level[1].forward
        i = i + 1
        print("rank ".. i .. "=> value " .. x.value .. ", key " .. x.key)
    end
end

----------------------------zset-----------------------------

--[[
    @desc:          默认的比较函数。
    --@a:
	--@b:
    @return:
]]
local function defaultComFunc(a, b)
    if a == b then
        return 0
    end
    return a > b and 1 or -1
end

--[[
    @desc:          构造
    --@comFunc:     比较函数
    @return:
]]
function zset_meta:new(comFunc)
    if not comFunc or type(comFunc) ~= "function" then
        comFunc = defaultComFunc
    end
    return setmetatable({
        sl   = createSkipList(comFunc),
        objs = {},
    }, zset_meta)
end

--[[
    @desc:          value可以是自定义结构, 自己定义好比较结构就行
    --@key:
	--@value:
    @return:
]]
function zset_meta:add(value,key)
    if not key or not value then
        return
    end
    local old = self.objs[key]
    if old then
        self.sl:delete(key, old)
    end
    self.sl:insert(key, value)
    self.objs[key] = value
end

--[[
    @desc:          通过key值获取对应value
    --@key:
    @return:
]]
function zset_meta:getByKey(key)
    return self.objs[key]
end


--[[
    @desc:          通过排名获取对应值
    --@rank:
    @return:
]]
function zset_meta:getByRank(rank)
    local node = self.sl:getNodeByRank(rank)
    if not node then
        return
    end
    return node.key, node.value
end

--[[
    @desc:          通过key值获取对应排名
    --@key:
    @return:
]]
function zset_meta:rank(key)
    local value = self.objs[key]
    if not value then
        return nil
    end
    local rank = self.sl:getRank(key, value)
    return  rank > 0 and rank
end

--[[
    @desc:          反向通过key值获取对应排名
    --@key:
    @return:
]]
function zset_meta:rev_rank(key)
    local r = self:rank(key)
    if r then
        return self.sl:reverse_rank(r)
    end
    return r
end

--[[
    @desc:          获取排名数量
    @return:
]]
function zset_meta:count()
    return self.sl:get_count()
end

--[[
    @desc:          获取排行区间
    --@start:       开始排名
	--@stop:        结束排名
	--@withValue:   返回值是否包括value
    @return:
    withValue为false或nil时：
        res = {key1, key2, key3, key4}
    withValue为true时:
        res = {
            {
                key = key1,
                value = value1,
            },
            {
                key = key2,
                value = value2
            }
            ...
        }
]]
function zset_meta:range(start, stop, withValue)
    if start < 1 then start = 1 end
    if stop < 1 then stop = 1 end
    local res = self.sl:rank_range(start, stop)
    if not withValue then return res end
    local res1 = {}
    for _, v in ipairs(res) do
        assert(self.objs[v])
        res1[#res1+1] = {key = v, value = self.objs[v] }
    end
    return res1
end

--[[
    @desc:          根据key删除元素
    --@key:
    @return:
]]
function zset_meta:rem(key)
    local old = self.objs[key]
    if old then
        self.sl:delete(key, old)
        self.objs[key] = nil
    end
end


--[[
    @desc:          删除所有元素
    @return:
]]
function zset_meta:remAll()
    for _, v in pairs(self:getAll()) do
        self:rem(v)
    end
end

--[[
    @desc:          获取全部数据
    --@withValue:   获取的值是否包括value, 默认只返回key
    @return:
]]
function zset_meta:getAll(withValue)
    return self:range(1, self:count(), withValue)
end

function zset_meta:rev_range(r1, r2, withValue)
    local start = self.sl:reverse_rank(r1)
    local stop = self.sl:reverse_rank(r2)
    return self:range(start, stop, withValue)
end

function zset_meta:range_by_score(s1, s2)
    return self.sl:get_score_range(s1, s2)
end

function zset_meta:score(key)
    return self.objs[key]
end

function zset_meta:limit(count)
    local total = self.sl:get_count()
    if total <= count then
        return 0
    end
    return self.sl:deleteByRank(count+1, total, function (key)
        self.objs[key] = nil
    end)
end

function zset_meta:rev_limit(count)
    local total = self.sl:get_count()
    if total <= count then
        return 0
    end
    local from = self.sl:reverse_rank(count+1)
    local to   = self.sl:reverse_rank(total)
    return self.sl:deleteByRank(from, to, function (key)
        self.objs[key] = nil
    end)
end

--[[
    @desc:          从头开始打印所有数据
    @return:
]]
function zset_meta:dump()
    self.sl:dump()
end

return zset
