-- Retrieve the data for a throttled resource
function ReqlessThrottle:data()
  -- Default values for the data
  local data = {
    id = self.id,
    maximum = 0
  }

  -- Retrieve data stored in redis
  local throttle = redis.call('hmget', ReqlessThrottle.ns .. self.id, 'id', 'maximum')

  if throttle[2] then
    data.maximum = tonumber(throttle[2])
  end

  return data
end

-- Like data, but includes ttl.
function ReqlessThrottle:dataWithTtl()
  local data = self:data()
  data.ttl = self:ttl()
  return data
end

-- Set the data for a throttled resource
function ReqlessThrottle:set(data, expiration)
  redis.call('hmset', ReqlessThrottle.ns .. self.id, 'id', self.id, 'maximum', data.maximum)
  if expiration > 0 then
    redis.call('expire', ReqlessThrottle.ns .. self.id, expiration)
  end
end

-- Delete a throttled resource
function ReqlessThrottle:unset()
  redis.call('del', ReqlessThrottle.ns .. self.id)
end

-- Acquire a throttled resource for a job.
-- Returns true of the job acquired the resource, false otherwise
function ReqlessThrottle:acquire(jid)
  if not self:available() then
    return false
  end

  self.locks.add(1, jid)
  return true
end

function ReqlessThrottle:pend(now, jid)
  self.pending.add(now, jid)
end

-- Releases the lock taken by the specified jid.
-- number of jobs released back into the queues is determined by the locks_available method.
function ReqlessThrottle:release(now, jid)
  -- Only attempt to remove from the pending set if the job wasn't found in the
  -- locks set
  if self.locks.remove(jid) == 0 then
    self.pending.remove(jid)
  end

  local available_locks = self:locks_available()
  if self.pending.length() == 0 or available_locks < 1 then
    return
  end

  -- subtract one to ensure we pop the correct amount. peek(0, 0) returns the first element
  -- peek(0,1) return the first two.
  for _, jid in ipairs(self.pending.peek(0, available_locks - 1)) do
    local job = Reqless.job(jid)
    local data = job:data()
    local queue = Reqless.queue(data['queue'])

    queue.throttled.remove(jid)
    queue.work.add(now, data.priority, jid)
  end

  -- subtract one to ensure we pop the correct amount. pop(0, 0) pops the first element
  -- pop(0,1) pops the first two.
  local popped = self.pending.pop(0, available_locks - 1)
end

-- Returns true if the throttle has locks available, false otherwise.
function ReqlessThrottle:available()
  return self.maximum == 0 or self.locks.length() < self.maximum
end

-- Returns the TTL of the throttle
function ReqlessThrottle:ttl()
  return redis.call('ttl', ReqlessThrottle.ns .. self.id)
end

-- Returns the number of locks available for the throttle.
-- calculated by maximum - locks.length(), if the throttle is unlimited
-- then up to 10 jobs are released.
function ReqlessThrottle:locks_available()
  if self.maximum == 0 then
    -- Arbitrarily chosen value. might want to make it configurable in the future.
    return 10
  end

  return self.maximum - self.locks.length()
end
