local ID        = 1
local CREATED   = 2
local KEY       = 3
local DATA      = 4

local log = require 'log'
local fiber = require 'fiber'
local pack_key = require 'lp.pack_key'

local lp = {
    VERSION                 = '1.0',

    defaults = {
        expire_timeout          = 1800,
        serialize_key_method    = 'none',
        lsn_check_interval      = 1,
        mode                    = 'master',
    },

    private     = {
        migrations      = require('lp.migrations'),
        waiter          = {},

        cond_run        = { instance = true, fiber = {} },

        first_id        = nil,
        last_id         = nil,
        processed_id    = nil,
    },
}

lp.private._pack = pack_key.none.pack
lp.private._unpack = pack_key.none.unpack

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

    local res
    if max ~= nil then
        res = max[ID]
    else
        res = tonumber64(0)
    end

    if self.private.last_id == nil then
        self.private.last_id = res
    end

    if self.private.last_id < res then
        self.private.last_id = res
    end

    return self.private.last_id
end

function lp:_first_id()
    local min = box.space.LP.index.id:min()
    if min == nil then
        if self.private.first_id == nil then
            self.private.first_id = 0
        end
    else
        self.private.first_id = min[ID]
    end
    return self.private.first_id
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
        table.insert(result,
            tuple:transform(KEY, 1, self.private._unpack(tuple[KEY])))
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
        fiber.name('LP-lsn')
        self.private.processed_id = self:_last_id()
        
        log.info(
            'LP: fiber `lsn` started (last_id = %s)',
            self.private.processed_id
        )

        local pause = self.opts.lsn_check_interval

        local json = require 'json'
        while cond.instance do
            local lsn = tonumber64(0)
            for _, lsn_s in pairs(box.info.vclock) do
                lsn = lsn + tonumber64(lsn_s)
            end
            if self.opts.mode ~= 'master' then
                -- update last_id
                self:_last_id()
            end

            if self.private.lsn ~= lsn then
                self.private.lsn = lsn

                while true do
                    local task = box.space.LP:get(
                        self.private.processed_id + tonumber64(1)
                    )
                    if task then
                        self.private.processed_id = task[ID]
                        self:_wakeup_consumers(task[KEY])
                    else
                        if self.private.processed_id == self:_last_id() then
                            break
                        end
                        self.private.processed_id =
                            self.private.processed_id + tonumber64(1)
                    end
                end
            end

            cond.fiber.lsn = fiber.id()
            fiber.sleep(pause)
            if cond.fiber.lsn then
                pause = self.opts.lsn_check_interval
            else
                -- someone woke up us
                -- on_replace - is BEFORE trigger
                pause = 0.05
                if pause > self.opts.lsn_check_interval then
                    pause = self.opts.lsn_check_interval
                end
            end
            cond.fiber.lsn = nil
        end
    end)
end

function lp:_expire_fiber()
    if self.opts.mode ~= 'master' then
        log.info('LP: expired is disabled: mode=%s', self.opts.mode)
        return
    end

    local cond = self.private.cond_run
    fiber.create(function()
        fiber.name('LP-expired')
        log.info('LP: fiber `expired` started')
        while cond.instance do
            local pause
            local task = box.space.LP.index.id:min()
            if task ~= nil then
                pause = fiber.time() - task[CREATED]
                pause = pause - self.opts.expire_timeout
                if pause >= 0 then
                    self.private.first_id = tonumber64(task[ID]) + tonumber64(1)
                    box.space.LP:delete(task[ID])
                    pause = nil
                else
                    pause = -pause
                end
            else
                pause = self.opts.expire_timeout
            end
            if pause then
                fiber.sleep(pause)
            end
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
        table.insert(keys, self.private._pack(k))
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
    key = self.private._pack(key)
    local time = fiber.time()
    local task = box.space.LP:insert{ self:_last_id() + 1, fiber.time(), key, data }
    return task:transform(KEY, 1, self.private._unpack(task[KEY]))
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
        self:push(key, data)
    end

    return count
end

function lp:init(defaults)
    local opts = self:_extend(self.defaults, defaults)

    self.opts = opts
    local upgrades = 0

    if self.opts.mode == 'master' then
        upgrades = self.private.migrations:upgrade(self)
    else
        while not box.space.LP do
            log.info('Wait for master process will create space')
            fiber.sleep(2)
        end
    end
    log.info('LP (%s) started', self.opts.mode)

    for _, trg in pairs(box.space.LP:on_replace()) do
        box.space.LP:on_replace(nil, trg)
    end

    -- stop old fibers
    self.private.cond_run.instance = false
    for _, fid in pairs(self.private.cond_run.fiber) do
        fiber.find(fid):wakeup()
    end


    if pack_key[self.opts.serialize_key_method] == nil then
        self.opts.serialize_key_method = 'none'
    end
    log.info('LP: use "%s" method to pack key', self.opts.serialize_key_method)
    self.private._pack = pack_key[self.opts.serialize_key_method].pack
    self.private._unpack = pack_key[self.opts.serialize_key_method].unpack


    -- condition for new fibers
    self.private.cond_run = { instance = true, fiber = {} }

    -- start lsn fiber
    self:_lsn_fiber()

    box.space.LP:on_replace(function(old, new)
        local fid = self.private.cond_run.fiber.lsn

        -- delete tuple
        if old ~= nil and new == nil then
            self.private.first_id = old[ID] + tonumber64(1)
        end
        if fid then
            self.private.cond_run.fiber.lsn = nil
            fiber.find(fid):wakeup()
        end
    end)

    -- start expire fiber
    self:_expire_fiber()

    for _, key in pairs(self.private.waiter) do
        self:_wakeup_consumers(key)
    end

    return upgrades
end

local private = {}
local public = {}

for k, v in pairs(lp) do
    if string.match(k, '^_') then
        private[k] = v
    else
        public[k] = v
    end
end
setmetatable(public, { __index = private })

return public

