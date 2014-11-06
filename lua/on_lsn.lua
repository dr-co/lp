local triggers = {}

local last_log_time = 0
local log_min_delay = 1
local watch_period  = 0.1

local last_lsn = box.info.lsn

box.on_change_lsn = function(cb)
    table.insert(triggers, cb)
end


local function log(m, force)
    if force == nil or not force then
        if box.time() - last_log_time < log_min_delay then
            return
        end
    end
    last_log_time = box.time()
    print(e)
end


local function watcher()
    box.fiber.name("on_lsn")

    while true do
        box.fiber.sleep(watch_period)
        if box.info.lsn ~= last_lsn then
            last_lsn = box.info.lsn

            for i, cb in pairs(triggers) do

                print('on_lsn')
                local s, e = pcall(cb, last_lsn)
                if not s then
                    log(e)
                end
            end
        end
    end
end


box.fiber.wrap(watcher)

return { on_change_lsn = box.on_change_lsn }
