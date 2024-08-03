-------------------------------------------------------------------------------
-- Configuration interactions
-------------------------------------------------------------------------------

-- This represents our default configuration settings
Reqless.config.defaults = {
  ['application']        = 'reqless',
  ['grace-period']       = '10',
  ['heartbeat']          = '60',
  ['jobs-history']       = '604800',
  ['jobs-history-count'] = '50000',
  ['max-job-history']    = '100',
  ['max-pop-retry']      = '1',
  ['max-worker-age']     = '86400',
}

-- Get one or more of the keys
Reqless.config.get = function(key, default)
  if key then
    return redis.call('hget', 'ql:config', key) or
      Reqless.config.defaults[key] or default
  end

  -- Inspired by redis-lua https://github.com/nrk/redis-lua/blob/version-2.0/src/redis.lua
  local reply = redis.call('hgetall', 'ql:config')
  for i = 1, #reply, 2 do
    Reqless.config.defaults[reply[i]] = reply[i + 1]
  end
  return Reqless.config.defaults
end

-- Set a configuration variable
Reqless.config.set = function(option, value)
  assert(option, 'config.set(): Arg "option" missing')
  assert(value , 'config.set(): Arg "value" missing')
  -- Send out a log message
  Reqless.publish('log', cjson.encode({
    event  = 'config_set',
    option = option,
    value  = value
  }))

  redis.call('hset', 'ql:config', option, value)
end

-- Unset a configuration option
Reqless.config.unset = function(option)
  assert(option, 'config.unset(): Arg "option" missing')
  -- Send out a log message
  Reqless.publish('log', cjson.encode({
    event  = 'config_unset',
    option = option
  }))

  redis.call('hdel', 'ql:config', option)
end
