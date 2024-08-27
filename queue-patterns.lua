local ReqlessQueuePatterns = {
  default_identifiers_default_pattern = '["*"]',
  default_priority_pattern = '{"fairly": false, "pattern": ["default"]}',
  ns = Reqless.ns .. "qp:",
}
ReqlessQueuePatterns.__index = ReqlessQueuePatterns

ReqlessQueuePatterns['getIdentifierPatterns'] = function(now)
  local reply = redis.call('hgetall', ReqlessQueuePatterns.ns .. 'identifiers')

  if #reply == 0 then
    -- Check legacy key
    reply = redis.call('hgetall', 'qmore:dynamic')
  end

  -- Include default pattern in case identifier patterns have never been set.
  local identifierPatterns = {
    ['default'] = ReqlessQueuePatterns.default_identifiers_default_pattern,
  }
  for i = 1, #reply, 2 do
    identifierPatterns[reply[i]] = reply[i + 1]
  end

  return identifierPatterns
end

-- Each key is a string and each value is string containing a JSON list of
-- patterns.
ReqlessQueuePatterns['setIdentifierPatterns'] = function(now, ...)
  if #arg % 2 == 1 then
    error('Odd number of identifier patterns: ' .. tostring(arg))
  end
  local key = ReqlessQueuePatterns.ns .. 'identifiers'

  local goodDefault = false;
  local identifierPatterns = {}
  for i = 1, #arg, 2 do
    local key = arg[i]
    local serializedValues = arg[i + 1]

    -- Ensure that the value is valid JSON.
    local values = cjson.decode(serializedValues)

    -- Only write the value if there are items in the list.
    if #values > 0 then
      if key == 'default' then
        goodDefault = true
      end
      table.insert(identifierPatterns, key)
      table.insert(identifierPatterns, serializedValues)
    end
  end

  -- Ensure some kind of default value is persisted.
  if not goodDefault then
    table.insert(identifierPatterns, "default")
    table.insert(
      identifierPatterns,
      ReqlessQueuePatterns.default_identifiers_default_pattern
    )
  end

  -- Clear out the legacy key too
  redis.call('del', key, 'qmore:dynamic')
  redis.call('hset', key, unpack(identifierPatterns))
end

ReqlessQueuePatterns['getPriorityPatterns'] = function(now)
  local reply = redis.call('lrange', ReqlessQueuePatterns.ns .. 'priorities', 0, -1)

  if #reply == 0 then
    -- Check legacy key
    reply = redis.call('lrange', 'qmore:priority', 0, -1)
  end

  if #reply == 0 then
    reply = {ReqlessQueuePatterns.default_priority_pattern}
  end

  return reply
end

-- Each key is a string and each value is a string containing a JSON object
-- where the JSON object has a shape like:
-- {"fairly": true, "pattern": ["string", "string", "string"]}
ReqlessQueuePatterns['setPriorityPatterns'] = function(now, ...)
  local key = ReqlessQueuePatterns.ns .. 'priorities'
  redis.call('del', key)
  -- Clear out the legacy key
  redis.call('del', 'qmore:priority')

  if #arg > 0 then
    -- Check for the default priority pattern and add one if none is given.
    local found_default = false
    for i = 1, #arg do
      local pattern = cjson.decode(arg[i])['pattern']
      if #pattern == 1 and pattern[1] == 'default' then
        found_default = true
        break
      end
    end
    if not found_default then
      table.insert(arg, ReqlessQueuePatterns.default_priority_pattern)
    end

    redis.call('rpush', key, unpack(arg))
  end
end
