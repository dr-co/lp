package.path = box.cfg.script_dir .. '/../../lua/?.lua;' .. package.path

require 'on_lsn'

function test_str()
    return 'str'
end


local lsn = 0
box.on_change_lsn(function(new_lsn)
    lsn = tonumber(new_lsn)
end)

function test_lsn()
    return tostring(lsn)
end
