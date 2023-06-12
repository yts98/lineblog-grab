local urlcode = (loadfile "urlcode.lua")()
local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
JSON = (loadfile "JSON.lua")()

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local item_type = os.getenv("item_type")
local item_value = os.getenv("item_value")

if urlparse == nil or http == nil then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}

math.randomseed(os.time())

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

abort_item = function(item)
  abortgrab = true
  if not item then
    item = item_type .. ":" .. item_value
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
    target[item] = true
    return true
  end
  return false
end

allowed = function(url, parenturl)
  if string.match(url, "^https?://[^/]*line%-scdn%.net/")
    or string.match(url, "^https?://parts%.lineblog%.me/") then
    return true
  elseif string.match(url, "^lineblog://") then
    return false
  end

  -- item boundary
  if item_type == "b" then
    local url_blogName = string.match(url, "^https?://lineblog%.me/([^/]+)/")
    if url_blogName and string.len(url_blogName) >= 1 and url_blogName ~= item_value then
      return false
    elseif url_blogName == item_value and not string.match(parenturl, "^https?://lineblog%.me/") then
      return false
    elseif string.match(url, "^https://www%.lineblog%.me/tag/[^/]+$") then
      local tag = string.match(url, "^https://www%.lineblog%.me/tag/([^/]+)$")
      tag = urlcode.unescape(tag)
      -- print("Found tag " .. tag)
      discovered_items["t:" .. tag] = true
      return false
    elseif string.match(url, "^https?://[^/]*lineblog%.me/")
      or string.match(url, "^https?://blog%.line%-apps%.com/")
      or string.match(url, "^https?://blog%-api%.line%-apps%.com/") then
      return true
    end
  elseif item_type == "t" then
    if string.match(url, "^https?://www%.lineblog%.me/tag/")
      or string.match(url, "^https?://www%.lineblog%.me/api/tag/")
      or string.match(url, "^https?://blog%-api%.line%-apps%.com/v1/explore/tag") then
      return true
    end
  elseif item_type == "kw" then
    if string.match(url, "^https?://blog%-api%.line%-apps%.com/v1/search")
      or string.match(url, "^https?://blog%-api%.line%-apps%.com/v1/suggest") then
      return true
    end
  end

  -- blacklist:share
  if string.match(url, "^https?://www%.facebook%.com/share%.php%?")
    or string.match(url, "^https?://www%.facebook%.com/sharer%.php%?")
    or string.match(url, "^https?://www%.facebook%.com/sharer/sharer%.php%?")
    or string.match(url, "^https?://twitter%.com/intent/tweet%?")
    or string.match(url, "^https?://twitter%.com/share%?") then
    return false
  end

  if not string.match(url, "^/[^/]")
    and not string.match(url, "^https?://[^/]*lineblog%.me/")
    and not string.match(url, "^https?://[^/]*line%-apps%.com/") then
    discover_item(discovered_outlinks, url)
  end
  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  if downloaded[url] ~= true and addedtolist[url] ~= true
     and allowed(url, parent["url"]) and not processed(url) then
    addedtolist[url] = true
    return true
  end

  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil

  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function fix_case(newurl)
    if not string.match(newurl, "^https?://.") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function check(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and not processed(url_ .. "/")
      and allowed(url_, origurl) then
      table.insert(urls, { url=url_ })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checkref(newurl, referer)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and not processed(url_ .. "/")
      and allowed(url_, origurl) then
      table.insert(urls, { url=url_, headers={ ["Referer"] = referer } })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checkXHR(newurl, referer)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and not processed(url_ .. "/")
      and allowed(url_, origurl) then
      table.insert(urls, { url=url_, headers={ ["Accept"] = "application/json, text/javascript, */*; q=0.01", ["Referer"] = referer, ["X-Requested-With"] = "XMLHttpRequest" } })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local blogName = string.match(url, "^https://blog%-api%.line%-apps%.com/v1/blog/([^/]+)")
  local articleId = string.match(url, "^https://blog%-api%.line%-apps%.com/v1/blog/[^/]+/article/([0-9]+)/")

  if item_type == "b" then
    html = read_file(file)
    if string.match(url, "^https://blog%-api%.line%-apps%.com/v1/blog/[^/]+/articles") then
      local json = JSON:decode(html)
      assert(blogName)
      assert(json["status"])
      if json["status"] == 200 then
        assert(json["data"] and json["data"]["blog"] and json["data"]["rows"])
        assert(json["data"]["blog"]["url"])
        if json["data"]["nextPageKey"] then
          assert(string.len(json["data"]["nextPageKey"]) >= 1)
          check("https://blog-api.line-apps.com/v1/blog/" .. blogName .. "/articles?withBlog=1&pageKey=" .. json["data"]["nextPageKey"])
        end
        for _, article in pairs(json["data"]["rows"]) do
          assert(article["id"] and article["url"])
          check("https://blog-api.line-apps.com/v1/blog/" .. blogName .. "/article/" .. article["id"] .. "/like/list")
          check("https://blog-api.line-apps.com/v1/blog/" .. blogName .. "/article/" .. article["id"] .. "/reblog/list")
          check("https://blog-api.line-apps.com/v1/blog/" .. blogName .. "/article/" .. article["id"] .. "/comment/list")
          check("https://blog-api.line-apps.com/v1/blog/" .. blogName .. "/article/" .. article["id"] .. "/info")
          if article["articleImageUrl"] then
            check(article["articleImageUrl"])
          end
          if article["inAppUrl"] then
            -- com.linecorp.lineblog.network.RequestHeaderUtil.createCustomHeaders()
            -- com.linecorp.lineblog.util.UserAgentUtil.getBlogUserAgent()
            table.insert(urls, { url=article["inAppUrl"], headers={ ["User-Agent"] = "LineBlog/1.7.8 (Linux; U; Android 13; Pixel 6a Build/SD2A.220601.001.B1)", ["Accept-Language"] = "ja" } })
            addedtolist[article["inAppUrl"]] = true
          end
          -- check(article["url"])
        end
        if json["data"]["blog"]["headerImageUrl"] then
          check(json["data"]["blog"]["headerImageUrl"])
        end
        if json["data"]["blog"]["iconUrl"] then
          check(json["data"]["blog"]["iconUrl"])
        end
        if json["data"]["blog"]["member"] and json["data"]["blog"]["member"]["profileIconUrl"] then
          check(json["data"]["blog"]["member"]["profileIconUrl"])
        end
        -- check(json["data"]["blog"]["url"])
      end
    elseif string.match(url, "^https://blog%-api%.line%-apps%.com/v1/blog/[^/]+/follow[ers]*/list") then
      local json = JSON:decode(html)
      local fol_direction = string.match(url, "^https://blog%-api%.line%-apps%.com/v1/blog/[^/]+/(follow[ers]*)/list")
      assert(blogName)
      assert(fol_direction == "followers" or fol_direction == "follow")
      assert(json["status"])
      if json["status"] == 200 then
        assert(json["data"] and json["data"]["rows"])
        if json["data"]["nextPageKey"] then
          assert(string.len(json["data"]["nextPageKey"]) >= 1)
          check("https://blog-api.line-apps.com/v1/blog/" .. blogName .. "/" .. fol_direction .. "/list?pageKey=" .. json["data"]["nextPageKey"])
        end
        for _, fol in pairs(json["data"]["rows"]) do
          assert(fol["name"] and string.len(fol["name"]) >= 1)
          -- print("Found blog " .. fol["name"])
          discovered_items["b:" .. fol["name"]] = true
        end
      end
    elseif string.match(url, "^https://blog%-api%.line%-apps%.com/v1/blog/[^/]+/article/[0-9]+/[a-z]+/list") then
      local json = JSON:decode(html)
      local list_type = string.match(url, "^https://blog%-api%.line%-apps%.com/v1/blog/[^/]+/article/[0-9]+/([a-z]+)/list")
      assert(blogName and articleId)
      assert(list_type == "comment" or list_type == "reblog" or list_type == "like")
      assert(json["status"])
      if json["status"] == 200 then
        assert(json["data"] and json["data"]["rows"])
        if json["data"]["nextPageKey"] then
          assert(string.len(json["data"]["nextPageKey"]) >= 1)
          check("https://blog-api.line-apps.com/v1/blog/" .. blogName .. "/article/" .. articleId .. "/" .. list_type .. "/list?pageKey=" .. json["data"]["nextPageKey"])
        end
        for _, item in pairs(json["data"]["rows"]) do
          assert(item["id"])
          if list_type == "comment" then
            assert(item["member"])
            if item["member"]["blog"] then
              assert(item["member"]["blog"] and item["member"]["blog"]["name"] and string.len(item["member"]["blog"]["name"]) >= 1)
              assert(item["member"]["name"])
              -- print("Found blog " .. item["member"]["blog"]["name"])
              discovered_items["b:" .. item["member"]["blog"]["name"]] = true
            end
          elseif list_type == "reblog" or list_type == "like" then
            assert(item["blog"] and item["blog"]["name"] and string.len(item["blog"]["name"]) >= 1)
            -- print("Found blog " .. item["blog"]["name"])
            discovered_items["b:" .. item["blog"]["name"]] = true
          end
        end
      end
    end
  elseif item_type == "t" then
    html = read_file(file)
    if string.match(url, "^https://blog%-api%.line%-apps%.com/v1/explore/tag") then
      local json = JSON:decode(html)
      assert(json["status"])
      if json["status"] == 200 then
        assert(json["data"] and json["data"]["tag"] and json["data"]["tag"]["name"] and json["data"]["rows"])
        if json["data"]["nextPageKey"] then
          assert(string.len(json["data"]["nextPageKey"]) >= 1)
          if json["data"]["nextPageKey"] ~= "1001" then
            check("https://blog-api.line-apps.com/v1/explore/tag?tag=" .. json["data"]["tag"]["name"] .. "&pageKey=" .. json["data"]["nextPageKey"] .. "&withTag=1")
          end
        end
        for _, article in pairs(json["data"]["rows"]) do
          assert(article["blog"] and article["blog"]["name"] and string.len(article["blog"]["name"]) >= 1)
          -- print("Found blog " .. article["blog"]["name"])
          discovered_items["b:" .. article["blog"]["name"]] = true
        end
      end
    elseif string.match(url, "^https://www%.lineblog%.me/tag/[^/]+$") then
      local tag = string.match(url, "^https://www%.lineblog%.me/tag/([^/]+)$")
      assert(tag)
      checkXHR("https://www.lineblog.me/api/tag/?tag=" .. tag .. "&blogName=&pageKey=1", url)
    elseif string.match(url, "^https://www%.lineblog%.me/api/tag/%?tag=[^&]+") then
      local json = JSON:decode(html)
      assert(json["status"])
      if json["status"] == "success" then
        assert(json["tag"] and json["tag"]["name"] and json["rows"])
        if json["nextPageKey"] then
          assert(string.len(json["nextPageKey"]) >= 1)
          if json["nextPageKey"] ~= "1001" then
            checkXHR("https://www.lineblog.me/api/tag/?tag=" .. json["tag"]["name"] .. "&blogName=&pageKey=" .. json["nextPageKey"], "https://www.lineblog.me/tag/" .. urlcode.escape(json["tag"]["name"]))
          end
        end
        for _, article in pairs(json["rows"]) do
          assert(article["blog"] and article["blog"]["name"] and string.len(article["blog"]["name"]) >= 1)
          -- print("Found blog " .. article["blog"]["name"])
          discovered_items["b:" .. article["blog"]["name"]] = true
        end
      end
    end
  elseif item_type == "kw" then
    html = read_file(file)
    if string.match(url, "^https://blog%-api%.line%-apps%.com/v1/[a-z]+/[a-z]+") then
      local json = JSON:decode(html)
      local method = string.match(url, "^https://blog%-api%.line%-apps%.com/v1/([a-z]+/[a-z]+)")
      assert(method == "search/articles" or method == "search/tags" or method == "search/users" or method == "suggest/tags")
      assert(json["status"])
      if json["status"] == 200 then
        assert(json["data"] and json["data"]["query"] and string.len(json["data"]["query"]) >= 1 and json["data"]["rows"])
        if json["data"]["nextPageKey"] then
          assert(string.len(json["data"]["nextPageKey"]) >= 1)
          -- because of https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules.html#index-max-result-window
          if not ((method ~= "suggest/tags" and json["data"]["nextPageKey"] == "501") or (method == "suggest/tags" and json["data"]["nextPageKey"] == "1001")) then
            check("https://blog-api.line-apps.com/v1/" .. method .. "?keyword=" .. json["data"]["query"] .. "&pageKey=" .. json["data"]["nextPageKey"])
          end
        end
        for _, item in pairs(json["data"]["rows"]) do
          if method == "search/articles" then
            assert(item["blog"] and item["blog"]["name"] and string.len(item["blog"]["name"]) >= 1)
            -- print("Found blog " .. item["blog"]["name"])
            discovered_items["b:" .. item["blog"]["name"]] = true
          elseif method == "search/tags" then
            assert(item["name"] and item["articles"])
            for _, article in pairs(item["articles"]) do
              assert(article["blog"] and article["blog"]["name"] and string.len(article["blog"]["name"]) >= 1)
              -- print("Found blog " .. article["blog"]["name"])
              discovered_items["b:" .. article["blog"]["name"]] = true
            end
            -- print("Found tag " .. item["name"])
            discovered_items["t:" .. item["name"]] = true
          elseif method == "search/users" then
            assert(item["blog"] and item["blog"]["name"] and string.len(item["blog"]["name"]) >= 1)
            assert(item["name"])
            -- print("Found blog " .. item["blog"]["name"])
            discovered_items["b:" .. item["blog"]["name"]] = true
          elseif method == "suggest/tags" then
            -- print("Found tag " .. item["name"])
            discovered_items["t:" .. item["name"]] = true
          end
        end
      end
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if status_code ~= 200
    and status_code ~= 301
    and status_code ~= 302
    and status_code ~= 403
    and status_code ~= 404 then
    io.stdout:write("Server returned bad response. Skipping.\n")
    io.stdout:flush()
    kill_grab()
  end
  if abortgrab then
    return false
  end
  if item_type == "kw" then
    return false
  end
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end

  if status_code < 400 then
    downloaded[url["url"]] = true
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 then
    io.stdout:write("Server returned bad response.")
    io.stdout:flush()
    tries = tries + 1
    if tries > 8 then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    io.stdout:write(" Sleeping.\n")
    io.stdout:flush()
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 10
    while tries < maxtries do
      if killgrab then
        return false
      end
      io.stdout:write("http://tracker/backfeed/legacy/" .. key .. "\n")
      local body, code, headers, status = http.request(
        "http://tracker/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and JSON:decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()

  for key, data in pairs({
    ["lineblog-0000000000000000"] = discovered_items,
    ["urls-0000000000000000"] = discovered_outlinks
  }) do
    print('queuing for', string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 100 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end

