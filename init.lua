package.path = box.cfg.script_dir .. '/lua/?.lua;' .. package.path
lp = (require 'lp_old').new(0, 2)
lp2 = (require 'lp_old').new(0, 2, true)
