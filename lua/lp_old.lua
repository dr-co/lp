return {
    new = function(space, expire_timeout)
        local ID                = 0
        local TIME              = 1
        local KEY               = 2
        local DATA              = 3
        local lp = require 'lp'
        
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
        
        local self = lp.new(space, expire_timeout)

        -- old style put(klen, skey[1], .. skey[klen], data)
        self.put = function(klen, ...)
            klen = tonumber(klen)
            local key = {}
            for i = 1, klen do
                local e = select(i, ...)
                table.insert(key, e)
            end
            local data = select(klen + 1, ...)
            key = table.concat(key, '::')
            local event = self.push(key, data)

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

        self.take = function(id, timeout, ...)
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

            local events = self.subscribe(id, timeout, unpack(takeargs))

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

        return self
    end
}


