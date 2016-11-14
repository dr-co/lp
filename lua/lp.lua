local ID        = 1
local CREATED   = 2
local KEY       = 3
local DATA      = 4

local log = require 'log'
local fiber = require 'fiber'
local msgpack = require 'msgpack'


local lp = {
    VERSION                 = '1.0',

    defaults = {
        expire_timeout      = 1800,
        serialize_key       = true,
    },

    private     = {
        migrations      = require('lp.migrations'),
        processed_id    = 0,
        waiter          = {},

        cond_run        = { instance = true, fiber = {} },

        first_id        = nil,
        last_id         = nil,
    }
}


function lp:_extend(t1, t2)
    local res = {}
    if t1 ~= nil then
        for k, v in pairs(t1) do
            res[k] = v
        end
    end

    if t2 ~= nil then
        for k, v in pairs(t2) do
            if res[k] ~= nil and v ~= nil and type(res[k]) ~= type(v) then
                box.error(box.error.PROC_LUA,
                    string.format(
                        'Wrong type for ".%s": %s (have to be %s)',
                            tostring(k),
                            type(v),
                            type(res[k])
                    )
                )
            end
            res[k] = v
        end
    end
    return res
end

function lp:_last_id()
    local max = box.space.LP.index.id:max()
    if max ~= nil then
        self.private.last_id = max[ID]
    else
        -- empty space doesn't loose last_id
        if self.private.last_id == nil then
            self.private.last_id = tonumber64(0)
        end
    end
    return self.private.last_id
end

function lp:_first_id()
    if self.private.first_id ~= nil then
        return self.private.first_id
    end
    local min = box.space.LP.index.id:min()
    if min == nil then
        self.private.first_id = 0
    else
        self.private.first_id = min[ID]
    end
    return self.private.first_id
end


function lp:_put_task(key, data)
    if self.opts.serialize_key then
        key = msgpack.encode(key)
    end
    local time = fiber.time()
    local task = box.space.LP:insert{ self:_last_id() + 1, fiber.time(), key, data }

    -- wakeup expire fiber if it sleeps
    if self.private.cond_run.fiber.expire ~= nil then
        local fid = self.private.cond_run.fiber.expire
        self.private.cond_run.fiber.expire = nil
        fiber.find(fid):wakeup()
    end

    if self.opts.serialize_key then
        key = msgpack.decode(task[KEY])
        return task:transform(KEY, 1, key)
    else
        return task
    end
end

function lp:_take(id, keys)
    id = tonumber64(id)
    local res = {}

    for i, key in pairs(keys) do
        for _, tuple in box.space.LP.index.subscribe:pairs({ key, id }, { iterator ='GE' }) do
            if tuple[KEY] ~= key then
                break
            end
            table.insert(res, tuple)
        end
    end
    table.sort(res, function(a, b) return a[1] < b[1] end)
    local result = {}
    for _, tuple in pairs(res) do
        if self.opts.serialize_key then
            local key = msgpack.decode(tuple[KEY])
            table.insert(result, tuple:transform(KEY, 1, key))
        else
            table.insert(result, tuple)
        end
    end
    return result
end

function lp:_wakeup_consumers(key)
    if not self.private.waiter[key] then
        return
    end

    for fid, keys in pairs(self.private.waiter[key]) do
        if keys then
            for _, k in pairs(keys) do
                self.private.waiter[k][fid] = nil
            end
            fiber.find(fid):wakeup()
        end
    end
end

function lp:_lsn_fiber()
    local cond = self.private.cond_run
    fiber.create(function()
        while cond.instance do
            if box.info.server.lsn ~= self.private.lsn then
                self.private.lsn = box.info.server.lsn

                while true do
                    local task = box.space.LP:get(self.private.processed_id + tonumber64(1))
                    if not task then
                        break
                    end
                    self.private.processed_id = task[ID]
                    self:_wakeup_consumers(task[KEY])
                end
            end

            fiber.sleep(0.1)
        end
    end)
end

function lp:_expire_fiber()
    local cond = self.private.cond_run
    fiber.create(function()
        while cond.instance do
            local pause = 3600
            local task = box.space.LP.index.id:min()
            if task ~= nil then
                pause = fiber.time() - task[CREATED]
                pause = pause - self.opts.expire_timeout
                if pause >= 0 then
                    box.space.LP:delete(task[ID])
                    pause = 0
                else
                    pause = -pause
                end
            else
                cond.fiber.expire = fiber.id()
            end
            fiber.sleep(pause)
            cond.fiber.expire = nil
        end
    end)
end

-------------------------------------------------------------------------------
-- Public API
-------------------------------------------------------------------------------

function lp:subscribe(id, timeout, ...)
    
    local pkeys = {...}

    local keys = {}
    for i, k in pairs(pkeys) do
        if self.opts.serialize_key then
            table.insert(keys, msgpack.encode(k))
        else
            table.insert(keys, k)
        end
    end

    id = tonumber64(id)

    -- Инициализация: если 0 - сразу возвращаем ID/MinID
    -- Если id потерялся, то сразу отвечаем проблемой
    if id == tonumber64(0) or id < self:_first_id() then
        return {
            box.tuple.new{ self:_last_id() + tonumber64(1), self:_first_id() }
        }
    end

    local events = self:_take(id, keys)

    if #events > 0 then
        table.insert(
            events,
            box.tuple.new{ self:_last_id() + tonumber64(1), self:_first_id() }
        )
        return events
    end

    timeout = tonumber(timeout)
    local started
    local fid = fiber.id()

    while timeout > 0 do
        started = fiber.time()

        -- set waiter fid
        for _, key in pairs(keys) do
            if self.private.waiter[key] == nil then
                self.private.waiter[key] = {}
            end
            self.private.waiter[key][fid] = keys
        end

        fiber.sleep(timeout)

        for _, key in pairs(keys) do
            self.private.waiter[key][fid] = nil
        end

        timeout = timeout - (fiber.time() - started)

        events = self:_take(id, keys)
        if #events > 0 then
            break
        end
    end

    local last_id = self:_last_id()
    if id <= last_id then
        id = last_id + tonumber64(1)
    end

    table.insert(
        events,
        box.tuple.new{ self:_last_id() + tonumber64(1), self:_first_id() }
    )
    return events
end

function lp.push(self, key, data)
    return self:_put_task(key, data)
end

function lp.push_list(self, ...)
    local put = {...}
    local i = 1
    local count = 0

    while i <= #put do
        local key = put[ i ]
        local data = put[ i + 1 ]
        i = i + 2
        count = count + 1
        self:_put_task(key, data)
    end

    return count
end

function lp.init(self, defaults)
    self.opts = self:_extend(self.defaults, defaults)
    local upgrades = self.private.migrations:upgrade(self)
    log.info('LP started')

    -- stop old fibers
    self.private.cond_run.instance = false
    for _, fid in pairs(self.private.cond_run.fiber) do
        fiber.find(fid):wakeup()
    end

    -- condition for new fibers
    self.private.cond_run = { instance = true, fiber = {} }

    -- start lsn fiber
    self:_lsn_fiber()

    -- start expire fiber
    self:_expire_fiber()

    for _, key in pairs(self.private.waiter) do
        self:_wakeup_consumers(key)
    end

    return upgrades
end
return lp

-- require 'on_lsn'
-- return {
--     new = function(space, expire_timeout)
--         -- constants
--         local ID                = 0
--         local TIME              = 1
--         local KEY               = 2
--         local DATA              = 3
--         local EXPIRE_TIMEOUT    = 1800
-- 
--         space = tonumber(space)
--         if expire_timeout ~= nil then
--             expire_timeout = tonumber(expire_timeout)
--         else
--             expire_timeout = EXPIRE_TIMEOUT
--         end
-- 
--         local self              = {}
--         local chs               = {}    -- channels
--         local pool_chs          = {}    -- channel_pool
--         local waiters           = {}    -- waiters
-- 
-- 
--         local _last_id = tonumber64(0)
--         local last_id
--         local last_checked_id = tonumber64(0)
-- 
-- 
--         first_id = function()
--             local min = box.space[space].index[0]:min()
--             if min == nil then
--                 return 0
--             end
--             return box.unpack('l', min[ID])
--         end
-- 
--         last_id = function()
--             local max = box.space[space].index[0]:max()
--             if max == nil then
--                 return _last_id
--             end
--             _last_id = box.unpack('l', max[ID])
--             return _last_id
--         end
-- 
--         local function channel()
--             if #pool_chs < 1024 then
--                 return box.ipc.channel(1)
--             end
--             local ch = table.remove(pool_chs, 1)
--             if ch == nil then
--                 ch = box.ipc.channel(1)
--             end
--             return ch
--         end
--         local function drop_channel(id)
--             if chs[id] == nil then
--                 return
--             end
--             table.insert(pool_chs, chs[id])
--             chs[id] = nil
--         end
-- 
--         local function sprintf(fmt, ...) return string.format(fmt, ...) end
--         local function printf(fmt, ...) print(sprintf(fmt, ...)) end
-- 
--         local function _take(id, keys)
-- 
--             id = box.pack('l', id)
--             local res = {}
-- 
--             for i, key in pairs(keys) do
--                 local iter = box.space[space].index[1]
--                     :iterator(box.index.GE, key, id)
-- 
--                 for tuple in iter do
--                     if tuple[KEY] ~= key then
--                         break
--                     end
--                     table.insert(res, { box.unpack('l', tuple[ID]), tuple })
--                 end
--             end
--             table.sort(res, function(a, b) return a[1] < b[1] end)
--             local result = {}
-- 
--             for i, v in pairs(res) do
--                 table.insert(result, v[2])
--             end
-- 
--             return result
--         end
-- 
-- 
--         -- cleanup space iteration
--         local function cleanup_space()
-- 
--             local now = box.time()
--             local count = 0
--             while true do
--                 local iter = box.space[space].index[0]
--                                 :iterator(box.index.GE, box.pack('l', 0))
--                 local lst = {}
--                 for tuple in iter do
--                     if box.unpack('i', tuple[TIME]) + expire_timeout > now then
--                         break
--                     end
--                     table.insert(lst, tuple[ID])
--                     if #lst >= 1000 then
--                         break
--                     end
--                 end
-- 
--                 if #lst == 0 then
--                     break
--                 end
-- 
--                 for num, id in pairs(lst) do
--                     box.delete(space, id)
--                     count = count + 1
--                 end
--             end
--             return count
--         end
-- 
--         -- wakeup waiters
--         local function wakeup_waiters(key)
--             while waiters[key] ~= nil do
--                 local wlist = waiters[key]
--                 waiters[key] = nil
--                 -- wakeup waiters
--                 for fid in pairs(wlist) do
--                     wlist[fid] = nil
--                     if chs[fid] ~= nil then
--                         local ch = chs[fid]
--                         drop_channel(fid)
--                         ch:put(true)
--                     end
--                 end
--             end
--         end
-- 
-- 
--         local function on_change_lsn(lsn)
--             local tuple
--             while last_checked_id < last_id() do
--                 last_checked_id = last_checked_id + tonumber64(1)
--                 tuple = box.select(space, 0, box.pack('l', last_checked_id))
--                 if tuple ~= nil then
--                     wakeup_waiters(tuple[KEY])
--                 end
--             end
--         end
-- 
--         box.on_change_lsn(on_change_lsn)
-- 
--         local function put_task(key, data)
-- 
--             local time = box.pack('i', math.floor(box.time()))
-- 
--             local task
--             if data ~= nil then
--                 task = box.insert(space,
--                     box.pack('l', last_id() + 1), time, key, data)
--             else
--                 task = box.insert(space, box.pack('l', last_id() + 1), time, key)
--             end
-- 
--             return task
--         end
-- 
--         -- put task
--         self.push = function(key, data)
--             return put_task(key, data)
--         end
-- 
--         -- put some tasks
--         self.push_list = function(...)
--             local put = {...}
--             local i = 1
--             local count = 0
--             while i <= #put do
--                 local key = put[ i ]
--                 local data = put[ i + 1 ]
--                 i = i + 2
--                 count = count + 1
--                 put_task(key, data)
--             end
-- 
--             return box.pack('l', count)
--         end
-- 
--         -- subscribe tasks
--         self.subscribe = function(id, timeout, ...)
--             local keys = {...}
-- 
--             id = tonumber64(id)
-- 
--             -- Инициализация: если 0 - сразу возвращаем ID/MinID
--             -- Если id потерялся, то сразу отвечаем проблемой
--             if id == tonumber64(0) or id < first_id() then
--                 return {
--                     box.tuple.new{
--                         box.pack('l', last_id() + tonumber64(1)),
--                         box.pack('l', first_id())
--                     }
--                 }
--             end
-- 
--             local events = _take(id, keys)
-- 
--             if #events > 0 then
--                 table.insert(
--                     events,
--                     box.tuple.new{
--                         box.pack('l', last_id() + tonumber64(1)),
--                         box.pack('l', first_id())
--                     }
--                 )
--                 return events
--             end
-- 
--             timeout = tonumber(timeout)
--             local started
--             local fid = box.fiber.self():id()
-- 
--             while timeout > 0 do
--                 started = box.time()
-- 
--                 -- set waiter fid
--                 for i, key in pairs(keys) do
--                     if waiters[key] == nil then
--                         waiters[key] = {}
--                     end
--                     waiters[key][fid] = true
--                 end
-- 
--                 chs[ fid ] = channel()
--                 if chs[ fid ]:get(timeout) == nil then
--                     -- drop channel if nobody puts into
--                     drop_channel(fid)
--                 end
-- 
--                 -- clean waiter fid
--                 for i, key in pairs(keys) do
--                     if waiters[key] ~= nil then
--                         waiters[key][fid] = nil
-- 
--                         -- memory leak if app uses unique keys
--                         local empty = true
--                         for i in pairs(waiters[key]) do
--                             empty = false
--                             break
--                         end
--                         if empty then
--                             waiters[key] = nil
--                         end
--                     end
--                 end
-- 
-- 
--                 timeout = timeout - (box.time() - started)
-- 
--                 events = _take(id, keys)
--                 if #events > 0 then
--                     break
--                 end
--             end
-- 
--             if id <= last_id() then
--                 id = last_id() + tonumber64(1)
--             end
-- 
--             -- last tuple always contains time
--             table.insert(events, box.tuple.new{
--                 box.pack('l', id),
--                 box.pack('l', first_id())
--             })
--             return events
--         end
-- 
--         -- get/set expire_timeout
--         self.expire_timeout = function(new_timeout)
--             if new_timeout ~= nil then
--                 new_timeout = tonumber(new_timeout)
--                 expire_timeout = new_timeout
--             end
--             return tostring(expire_timeout)
--         end
-- 
-- 
--         self.stat = function()
--             local tuples = {}
--             local clients = 0
--             for i in pairs(chs) do
--                 clients = clients + 1
--             end
--             clients = tostring(clients)
-- 
--             local keys = 0
--             for i in pairs(waiters) do
--                 keys = keys + 1
--             end
-- 
--             table.insert(tuples, box.tuple.new{'pool_channels', tostring(#pool_chs)})
--             table.insert(tuples, box.tuple.new{'clients', clients})
--             table.insert(tuples,
--                 box.tuple.new{'expire_timeout', tostring(expire_timeout)})
--             table.insert(tuples, box.tuple.new{'work_keys', tostring(keys)})
--             return tuples
--         end
-- 
--         self.cleanup = function()
--             return cleanup_space()
--         end
-- 
-- 
--         -- cleanup process
--         box.fiber.wrap(
--             function()
--                 box.fiber.name('expired')
--                 printf("Start cleanup fiber for space %s (period %d sec): %s",
--                     space, expire_timeout, box.info.status)
--                 while true do
--                     if box.info.status == 'primary' then
--                         local min = box.space[space].index[0]:min()
--                         local now = math.floor( box.time() )
--                         if min ~= nil then
--                             local et =
--                                 box.unpack('i', min[TIME]) + expire_timeout
--                             if et <= now then
--                                 cleanup_space()
--                             end
--                         end
--                     end
--                     box.fiber.sleep(expire_timeout / 10)
--                 end
--             end
--         )
-- 
--         local max = box.space[space].index[0]:max()
--         if max ~= nil then
--             last_checked_id = box.unpack('l', max[ID])
--             max = nil
--         end
-- 
-- 
--         return self
--     end
-- }

