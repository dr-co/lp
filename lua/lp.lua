return {
    new = function(space, expire_timeout)
        -- constants
        local ID                = 0
        local TIME              = 1
        local KEY               = 2
        local DATA              = 3
        local EXPIRE_TIMEOUT    = 1800

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

        local function channel()
            if #pool_chs > 0 then
                local ch = pool_chs[ #pool_chs ]
                pool_chs[ #pool_chs ] = nil
                return ch
            end
            return box.ipc.channel(1)
        end
        local function drop_channel(id)
            if chs[id] == nil then
                return
            end
            table.insert(pool_chs, chs[id])
            chs[id] = nil
        end

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
                        drop_channel(fid)
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
            chs[ fid ] = channel()

            for i, key in pairs(keys) do
                if waiters[key] == nil then
                    waiters[key] = {}
                end
                waiters[key][fid] = true
            end

            chs[ fid ]:get(timeout)
            drop_channel(fid)
            
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
