local SCH_KEY       = 'drco_LP'

local log = require 'log'
local migrations = {}
migrations.list = {
    {
        up  = function()
            log.info('First start of LP detected')
        end
    },
    {
        description = 'Create main LP space',
        up  = function()
            box.schema.space.create(
                'LP',
                {
                    engine      = 'memtx',
                    format  = {
                        {                           -- #1
                            ['name']    = 'id',
                            ['type']    = 'unsigned',
                        },

                        {                           -- #2
                            ['name']    = 'created',
                            ['type']    = 'number',
                        },

                        {                           -- #3
                            ['name']    = 'key',
                            ['type']    = 'str',
                        },

                        {                           -- #4
                            ['name']    = 'data',
                            ['type']    = '*',
                        },
                    }
                }
            )
        end
    },
    
    {
        description = 'Create primary LP index',
        up  = function()
            box.space.LP:create_index(
                'id',
                {
                    unique  = true,
                    type    = 'tree',
                    parts   = { 1, 'unsigned' }
                }
            )
        end
    },

    {
        description = 'Create fetch LP index',
        up = function()
            box.space.LP:create_index(
                'fetch',
                {
                    unique  = false,
                    type    = 'tree',
                    parts   = { 3, 'str', 1, 'unsigned' }
                }
            )
        end
    }
}


function migrations.upgrade(self, mq)

    local db_version = 0
    local ut = box.space._schema:get(SCH_KEY)
    local version = mq.VERSION

    if ut ~= nil then
        db_version = ut[2]
    end

    local cnt = 0
    for v, m in pairs(migrations.list) do
        if db_version < v then
            local nv = string.format('%s.%03d', version, v)
            log.info('LP: up to version %s (%s)', nv, m.description)
            m.up(mq)
            box.space._schema:replace{ SCH_KEY, v }
            mq.VERSION = nv
            cnt = cnt + 1
        end
    end
    return cnt
end


return migrations

