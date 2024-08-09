local ReqlessQueuePatterns = {
  default_identifiers_default_pattern = '["*"]',
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
