-- cjson can't tell an empty array from an empty object, so empty arrays end up
-- encoded as objects. This function makes empty arrays look like empty arrays.
local function cjsonArrayDegenerationWorkaround(array)
  if #array == 0 then
    return "[]"
  end
  return cjson.encode(array)
end
