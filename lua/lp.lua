return {
    new = function(space, expire_timeout)
        -- constants
        local ID                = 0
        local TIME              = 1
        local KEY               = 2
        local DATA              = 3
        local EXPIRE_TIMEOUT    = 180

        space = tonumber(space)
        if expire_timeout ~= nil then
            expire_timeout = tonumber(expire_timeout)
        else
            expire_timeout = EXPIRE_TIMEOUT
        end

        local self              = {}
        local chs               = {}    -- channels
        local pool_chs          = {}    -- channel_pool
        local waiters           = {}    -- waiters
        local last_id           = tonumber64(0)
        

        local function sprintf(fmt, ...) return string.format(fmt, ...) end
        local function printf(fmt, ...) print(sprintf(fmt, ...)) end

        local function _take(id, keys)
            local res = {}

            for i, key in pairs(keys) do
                local iter = box.space[space].index[1]
                    :iterator(box.index.GE, key, id)

                for tuple in iter do
                    if tuple[KEY] ~= key then
                        break
                    end
                    table.insert(res, tuple)
                end
            end

            return res
        end

        -- split string
        local function strsplit(str, sep)
            local res = {}
            while true do
                local ss = string.match(str, '(.-)' .. sep)
                if ss == nil then
                    table.insert(res, str)
                    break
                end
                table.insert(res, ss)
                str = string.sub(str, string.len(ss) + string.len(sep) + 1)
            end
            return res 
        end

        -- cleanup space iteration
        local function cleanup_space()
            local iter = box.space[space].index[0]
                            :iterator(box.index.GE, box.pack('l', 0))

            local now = box.time()
            while true do
                local lst = {}
                for tuple in iter do
                    if box.unpack('i', tuple[TIME]) + expire_timeout > now then
                        break
                    end
                    table.insert(lst, tuple[ID])
                    if #lst >= 1000 then
                        break
                    end
                end

                if #lst == 0 then
                    break
                end

                for id, num in pairs(lst) do
                    box.delete(space, id)
                end
            end
        end

        -- wakeup waiters
        local function wakeup_waiters(key)
            if waiters[key] ~= nil then
                -- wakeup waiters
                for fid in pairs(waiters[key]) do
                    waiters[key][fid] = nil
                    if chs[fid] ~= nil then
                        chs[fid]:put(true)
                        chs[fid] = nil
                    end
                end
                waiters[key] = nil
            end
        end

        local function put_task(key, data, wakeup)
            last_id = last_id + tonumber64(1)

            local time = box.pack('i', math.floor(box.time()))

            local task
            if data ~= nil then
                task = box.insert(space,
                    box.pack('l', last_id), time, key, data)
            else
                task = box.insert(space, box.pack('l', last_id), time, key)
            end

            if wakeup then
                wakeup_waiters(key)
            end

            return task
        end

        -- put task
        self.put = function(key, data)
            return put_task(key, data, true)
        end

        -- put some tasks
        self.put_list = function(...)
            
            local put = {...}
            local keys = {}
            local i = 1
            while i <= #put do
                local key = put[ i ]
                local data = put[ i + 1 ]
                i = i + 2
                put_task(key, data, false)
                table.insert(keys, key)
            end

            for idx, key in pairs(keys) do
                wakeup_waiters(key)
            end
            return box.pack('l', #keys)
        end

        -- take tasks
        self.take = function(id, timeout, ...)
            local keys = {...}
            
            if tonumber64(id) == tonumber64(0) then
                id = last_id + tonumber64(1)
            end

            id = box.pack('l', tonumber64(id))
            
            timeout = tonumber(timeout)
            local events = _take(id, keys)
            
            if #events > 0 then
                table.insert(
                    events,
                    box.tuple.new{ box.pack('l', last_id + tonumber64(1)) }
                )
                return events
            end

            local fid = box.fiber.id()
            chs[ fid ] = box.ipc.channel(1)

            for i, key in pairs(keys) do
                if waiters[key] == nil then
                    waiters[key] = {}
                end
                waiters[key][fid] = true
            end

            chs[ fid ]:get(timeout)
            chs[ fid ] = nil
            
            events = _take(id, keys)
           
            for i, key in pairs(keys) do
                if waiters[key] ~= nil then
                    waiters[key][fid] = nil
                    local empty = false
                    for i in pairs(waiters[key]) do
                        empty = false
                        break
                    end
                    if empty then
                        waiters[key] = nil
                    end
                end
            end
            table.insert(
                events,
                box.tuple.new{ box.pack('l', last_id + tonumber64(1)) }
            )
            return events
        end

        -- get/set expire_timeout
        self.expire_timeout = function(new_timeout)
            if new_timeout ~= nil then
                new_timeout = tonumber(new_timeout)
                expire_timeout = new_timeout
            end
            return tostring(expire_timeout)
        end

        -- old style put(klen, skey[1], .. skey[klen], data)
        self.old_put = function(klen, ...)
            klen = tonumber(klen)
            local key = {}
            for i = 1, klen do
                local e = select(i, ...)
                table.insert(key, e)
            end
            local data = select(klen + 1, ...)
            key = table.concat(key, '::')
            local event = self.put(key, data)

            key = strsplit(event[KEY], '::')
            while #key < 5 do
                table.insert(key, '')
            end
            local time = box.pack('l', box.unpack('i', event[TIME]) * 1000000)

            table.insert(key, 'e')
            table.insert(key, box.pack('i', klen))
            table.insert(key, time)

            return event
                :transform(KEY, 1, unpack(key))
                :transform(TIME, 1)

        end

        self.old_take = function(id, timeout, ...)
            local args = { ... }
            local takeargs = {}
            while #args > 0 do
                local klen = tonumber(args[1])
                local key = {}
                for i = 1, klen do
                    table.insert(key, args[1 + i])
                end
                local data = args[1 + klen + 1]
                args = { select(klen + 2, unpack(args)) }
                table.insert(takeargs, table.concat(key, '::'))
                table.insert(takeargs, data)
            end

            local events = self.take(id, timeout, unpack(takeargs))

            for i, event in pairs(events) do
                local key
                local klen
                if #event == 1 then
                    key = { '', '', '', '', '' }
                    klen = 0
                else
                    key = strsplit(event[KEY], '::')
                    klen = #key
                    while #key < 5 do
                        table.insert(key, '')
                    end
                end
                local tuple = { event[ID], unpack(key) }
                table.insert(tuple, 'e')
                table.insert(tuple, klen)
                if #event == 1 then
                    table.insert(tuple, box.pack('l', box.time64()))
                    tuple[7] = 't'
                else
                    table.insert(tuple,
                        box.pack('l', 1000000 * box.unpack('i', event[TIME])))
                    table.insert(tuple, event[DATA])
                end
                events[i] = box.tuple.new(tuple)
            end
            return events
        end

        -- cleanup process
        box.fiber.wrap(
            function()
                printf("Starting cleanup fiber for space %s (period %d sec)",
                    space, expire_timeout)
                while true do
                    local min = box.space[space].index[0]:min()
                    local now = math.floor( box.time() )
                    if min ~= nil then
                        local et = box.unpack('i', min[TIME]) + expire_timeout
                        if et <= now then
                            cleanup_space()
                        end
                    end
                    box.fiber.sleep(expire_timeout / 10)
                end
            end
        )

        return self
    end
}
