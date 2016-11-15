local msgpack = require 'msgpack'
local json  = require 'json'

return {
    none = {
        pack = function(key)
            return key
        end,

        unpack = function(pkey)
            return pkey
        end
    },
    msgpack = {
        pack = function(key)
            return msgpack.encode(key)
        end,

        unpack = function(pkey)
            local key = msgpack.decode(pkey)
            return key
        end
    },
    json = {
        pack = function(key)
            return json.encode(key)
        end,

        unpack = function(pkey)
            return json.decode(pkey)
        end
    },

    colon = {
        pack = function(key)
            if type(key) == 'table' then
                return table.concat(key, ':')
            end
            return key .. ':'
        end,
        unpack = function(pkey)
            if string.match(pkey, ':$') then
                return string.sub(pkey, 1, string.len(pkey) - 1)
            end
            
            local res = {}
            while true do
                local pos = string.find(pkey, ':')
                if pos == nil then
                    table.insert(res, pkey)
                    return res
                end
                table.insert(res, string.sub(pkey, 1, pos - 1))
                pkey = string.sub(pkey, pos + 1)
            end
        end
    }
}

