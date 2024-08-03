-------------------------------------------------------------------------------
-- This is the analog of the 'main' function when invoking reqless directly, as
-- apposed to for use within another library
-------------------------------------------------------------------------------
local ReqlessAPI = {}

-- Where possible, use config.getAll to fetch all configs
ReqlessAPI['config.get'] = function(now, key)
  if key then
    return Reqless.config.get(key)
  end
  return ReqlessAPI['config.getAll'](now)
end

ReqlessAPI['config.getAll'] = function(now)
  return cjson.encode(Reqless.config.get(nil))
end

ReqlessAPI['config.set'] = function(now, key, value)
  Reqless.config.set(key, value)
end

-- Unset a configuration option
ReqlessAPI['config.unset'] = function(now, key)
  Reqless.config.unset(key)
end

ReqlessAPI['failureGroups.counts'] = function(now, start, limit)
  return cjson.encode(Reqless.failed(nil, start, limit))
end

ReqlessAPI['job.addDependency'] = function(now, jid, ...)
  return Reqless.job(jid):depends(now, "on", unpack(arg))
end

ReqlessAPI['job.addTag'] = function(now, jid, ...)
  local result = Reqless.tag(now, 'add', jid, unpack(arg))
  return cjsonArrayDegenerationWorkaround(result)
end

ReqlessAPI['job.cancel'] = function(now, ...)
  return Reqless.cancel(now, unpack(arg))
end

ReqlessAPI['job.complete'] = function(now, jid, worker, queue, data)
  return Reqless.job(jid):complete(now, worker, queue, data)
end

ReqlessAPI['job.completeAndRequeue'] = function(now, jid, worker, queue, data, next_queue, ...)
  return Reqless.job(jid):complete(now, worker, queue, data, 'next', next_queue, unpack(arg))
end

ReqlessAPI['job.fail'] = function(now, jid, worker, group, message, data)
  return Reqless.job(jid):fail(now, worker, group, message, data)
end

-- Return json for the job identified by the provided jid. If the job is not
-- present, then `nil` is returned
ReqlessAPI['job.get'] = function(now, jid)
  local data = Reqless.job(jid):data()
  if data then
    return cjson.encode(data)
  end
end

-- Return json blob of data or nil for each jid provided
ReqlessAPI['job.getMulti'] = function(now, ...)
  local results = {}
  for _, jid in ipairs(arg) do
    table.insert(results, Reqless.job(jid):data())
  end
  return cjsonArrayDegenerationWorkaround(results)
end

ReqlessAPI['job.heartbeat'] = function(now, jid, worker, data)
  return Reqless.job(jid):heartbeat(now, worker, data)
end

-- Add logging to a particular jid
ReqlessAPI['job.log'] = function(now, jid, message, data)
  assert(jid, "Log(): Argument 'jid' missing")
  assert(message, "Log(): Argument 'message' missing")
  if data then
    data = assert(cjson.decode(data),
      "Log(): Argument 'data' not cjson: " .. tostring(data))
  end

  local job = Reqless.job(jid)
  assert(job:exists(), 'Log(): Job ' .. jid .. ' does not exist')
  job:history(now, message, data)
end

ReqlessAPI['job.removeDependency'] = function(now, jid, ...)
  return Reqless.job(jid):depends(now, "off", unpack(arg))
end

ReqlessAPI['job.removeTag'] = function(now, jid, ...)
  local result = Reqless.tag(now, 'remove', jid, unpack(arg))
  return cjsonArrayDegenerationWorkaround(result)
end

ReqlessAPI['job.requeue'] = function(now, worker, queue, jid, klass, data, delay, ...)
  local job = Reqless.job(jid)
  assert(job:exists(), 'Requeue(): Job ' .. jid .. ' does not exist')
  return ReqlessAPI['queue.put'](now, worker, queue, jid, klass, data, delay, unpack(arg))
end

ReqlessAPI['job.retry'] = function(now, jid, queue, worker, delay, group, message)
  return Reqless.job(jid):retry(now, queue, worker, delay, group, message)
end

ReqlessAPI['job.setPriority'] = function(now, jid, priority)
  return Reqless.job(jid):priority(priority)
end

ReqlessAPI['job.timeout'] = function(now, ...)
  for _, jid in ipairs(arg) do
    Reqless.job(jid):timeout(now)
  end
end

ReqlessAPI['job.track'] = function(now, jid)
  return cjson.encode(Reqless.track(now, 'track', jid))
end

ReqlessAPI['job.untrack'] = function(now, jid)
  return cjson.encode(Reqless.track(now, 'untrack', jid))
end

ReqlessAPI["jobs.completed"] = function(now, offset, limit)
  local result = Reqless.jobs(now, 'complete', offset, limit)
  return cjsonArrayDegenerationWorkaround(result)
end

ReqlessAPI['jobs.failedByGroup'] = function(now, group, start, limit)
  return cjson.encode(Reqless.failed(group, start, limit))
end

ReqlessAPI['jobs.tagged'] = function(now, tag, ...)
  return cjson.encode(Reqless.tag(now, 'get', tag, unpack(arg)))
end

ReqlessAPI['jobs.tracked'] = function(now)
  return cjson.encode(Reqless.track(now))
end

ReqlessAPI['queue.counts'] = function(now, queue)
  return cjson.encode(ReqlessQueue.counts(now, queue))
end

ReqlessAPI['queue.forget'] = function(now, ...)
  ReqlessQueue.deregister(unpack(arg))
end

ReqlessAPI["queue.jobsByState"] = function(now, state, ...)
  local result = Reqless.jobs(now, state, unpack(arg))
  return cjsonArrayDegenerationWorkaround(result)
end

ReqlessAPI['queue.length'] = function(now, queue)
  return Reqless.queue(queue):length()
end

ReqlessAPI['queue.pause'] = function(now, ...)
  ReqlessQueue.pause(now, unpack(arg))
end

ReqlessAPI['queue.peek'] = function(now, queue, offset, limit)
  local jids = Reqless.queue(queue):peek(now, offset, limit)
  local response = {}
  for _, jid in ipairs(jids) do
    table.insert(response, Reqless.job(jid):data())
  end
  return cjsonArrayDegenerationWorkaround(response)
end

ReqlessAPI['queue.pop'] = function(now, queue, worker, limit)
  local jids = Reqless.queue(queue):pop(now, worker, limit)
  local response = {}
  for _, jid in ipairs(jids) do
    table.insert(response, Reqless.job(jid):data())
  end
  return cjsonArrayDegenerationWorkaround(response)
end

ReqlessAPI['queue.put'] = function(now, worker, queue, jid, klass, data, delay, ...)
  return Reqless.queue(queue):put(now, worker, jid, klass, data, delay, unpack(arg))
end

ReqlessAPI['queue.recurAtInterval'] = function(now, queue, jid, klass, data, interval, offset, ...)
  return Reqless.queue(queue):recurAtInterval(now, jid, klass, data, interval, offset, unpack(arg))
end

ReqlessAPI['queue.stats'] = function(now, queue, date)
  return cjson.encode(Reqless.queue(queue):stats(now, date))
end

ReqlessAPI['queue.throttle.get'] = function(now, queue)
  return ReqlessAPI['throttle.get'](now, ReqlessQueue.ns .. queue)
end

ReqlessAPI['queue.throttle.set'] = function(now, queue, max)
  Reqless.throttle(ReqlessQueue.ns .. queue):set({maximum = max}, 0)
end

ReqlessAPI['queue.unfail'] = function(now, queue, group, limit)
  assert(queue, 'queue.unfail(): Arg "queue" missing')
  return Reqless.queue(queue):unfail(now, group, limit)
end

ReqlessAPI['queue.unpause'] = function(now, ...)
  ReqlessQueue.unpause(unpack(arg))
end

ReqlessAPI['queues.counts'] = function(now)
  return cjsonArrayDegenerationWorkaround(ReqlessQueue.counts(now, nil))
end

ReqlessAPI['recurringJob.cancel'] = function(now, jid)
  return Reqless.recurring(jid):cancel()
end

ReqlessAPI['recurringJob.get'] = function(now, jid)
  local data = Reqless.recurring(jid):data()
  if data then
    return cjson.encode(data)
  end
end

ReqlessAPI['recurringJob.addTag'] = function(now, jid, ...)
  return Reqless.recurring(jid):tag(unpack(arg))
end

ReqlessAPI['recurringJob.removeTag'] = function(now, jid, ...)
  return Reqless.recurring(jid):untag(unpack(arg))
end

ReqlessAPI['recurringJob.update'] = function(now, jid, ...)
  return Reqless.recurring(jid):update(now, unpack(arg))
end

ReqlessAPI['tags.top'] = function(now, offset, limit)
  local result = Reqless.tag(now, 'top', offset, limit)
  return cjsonArrayDegenerationWorkaround(result)
end

ReqlessAPI['throttle.delete'] = function(now, tid)
  Reqless.throttle(tid):unset()
end

ReqlessAPI['throttle.get'] = function(now, tid)
  return cjson.encode(Reqless.throttle(tid):dataWithTtl())
end

ReqlessAPI['throttle.locks'] = function(now, tid)
  local result = Reqless.throttle(tid).locks.members()
  return cjsonArrayDegenerationWorkaround(result)
end

ReqlessAPI['throttle.pending'] = function(now, tid)
  local result = Reqless.throttle(tid).pending.members()
  return cjsonArrayDegenerationWorkaround(result)
end

-- releases the set of jids from the specified throttle.
ReqlessAPI['throttle.release'] = function(now, tid, ...)
  local throttle = Reqless.throttle(tid)

  for _, jid in ipairs(arg) do
    throttle:release(now, jid)
  end
end

ReqlessAPI['throttle.set'] = function(now, tid, max, ...)
  local expiration = unpack(arg)
  local data = {
    maximum = max
  }
  Reqless.throttle(tid):set(data, tonumber(expiration or 0))
end

ReqlessAPI['worker.forget'] = function(now, ...)
  ReqlessWorker.deregister(unpack(arg))
end

ReqlessAPI['worker.jobs'] = function(now, worker)
  return cjson.encode(ReqlessWorker.counts(now, worker))
end

ReqlessAPI['workers.counts'] = function(now)
  return cjsonArrayDegenerationWorkaround(ReqlessWorker.counts(now, nil))
end

-------------------------------------------------------------------------------
-- Function lookup
-------------------------------------------------------------------------------

-- None of the function calls accept keys
if #KEYS > 0 then error('No Keys should be provided') end

-- The first argument must be the function that we intend to call, and it must
-- exist
local command_name = assert(table.remove(ARGV, 1), 'Must provide a command')
local command      = assert(
  ReqlessAPI[command_name], 'Unknown command ' .. command_name)

-- The second argument should be the current time from the requesting client
local now          = tonumber(table.remove(ARGV, 1))
local now          = assert(
  now, 'Arg "now" missing or not a number: ' .. (now or 'nil'))

return command(now, unpack(ARGV))
