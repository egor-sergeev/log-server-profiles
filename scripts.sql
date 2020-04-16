select *, datetime
from user_actions_buffer
order by timestamp;

select uniq(user_id)
from user_actions_buffer;

select count()
from user_actions_buffer;


-- Table size
select concat(database, ''.'', table)                       AS table,
       formatReadableSize(sum(bytes))                       AS size,
       sum(bytes)                                           AS bytes_size,
       formatReadableSize(sum(primary_key_bytes_in_memory)) AS primary_keys_size,
       sum(rows)                                            AS rows,
       max(modification_time)                               AS latest_modification
from system.parts
where active
group by database, table
order by bytes_size desc;


-- Batch scrolls:
create view scrolls as
select user_id,
       object_type,
       null                                                                                     as object_id,
       action_type,
       sum(_value)                                                                              as value,
       min(_timestamp)                                                                          as timestamp,
       min(_datetime)                                                                           as datetime,
       arraySum(length(groupArray(_duration) as dur_arr) > 1 ? arrayPopBack(dur_arr) : dur_arr) as duration
from (
      select user_id,
             arrayPopBack(groupArray(object_type))            as _object_type,
             arrayPopBack(groupArray(object_id))              as _object_id,
             arrayPopBack(groupArray(action_type))            as _action_type,
             arrayPopBack(groupArray(value))                  as _value,
             arrayPopBack(groupArray(timestamp))              as _timestamp,
             arrayPopBack(groupArray(datetime))               as _datetime,
             arrayPopBack(groupArray(duration))               as _duration,
             arrayPopBack(arrayCumSum(groupArray(new_group))) as _group
      from (
            with 151 as const_scroll_gap_time_ms
            select user_id,
                   object_type,
                   object_id,
                   action_type,
                   value,
                   timestamp,
                   datetime,
                   neighbor(timestamp, 1, 0) - timestamp        as duration,
                   duration <= const_scroll_gap_time_ms ? 0 : 1 as new_group
            from user_actions_buffer
            where action_type = '' scroll ''
            order by user_id, timestamp
               )
      group by user_id
         )
    array join
     _object_type as object_type,
     _action_type as action_type,
     _value as _value,
     _timestamp as _timestamp,
     _datetime as _datetime,
     _duration as _duration,
     _group as group
group by user_id, group, object_type, action_type;


-- Batch clicks
create view clicks as
    with 10 * 60 * 1000 as click_afk_timeout
    select user_id,
           object_type,
           object_id,
           action_type,
           value,
           timestamp,
           datetime,
           duration
    from (
          select user_id,
                 object_type,
                 object_id,
                 action_type,
                 value,
                 timestamp,
                 datetime,
                 user_id = neighbor(user_id, 1) ? (neighbor(timestamp, 1) - timestamp) : 0 as duration
          from (
                select user_id, object_type, object_id, action_type, value, timestamp, datetime
                from user_actions_buffer
                order by user_id, timestamp
                   )
          where action_type = '' click ''
              or neighbor(action_type
              , -2) = '' click '' and neighbor(user_id
              , -2) = user_id
             )
    where action_type = ''click''
  and duration > 0
  and duration < click_afk_timeout;


-- Batch hovers
create view hovers as
    with 30 * 1000 as hover_afk_timeout
    select user_id,
           object_type,
           object_id,
           CAST('' hover '',
                '' Enum8(\''click\'' = 1, \''mouseover\'' = 2, \''mouseout\'' = 3, \''scroll\'' = 4, \''hover\'' = 5)
                '') as action_type,
           value,
           timestamp,
           datetime,
           duration
    from (
          select user_id,
                 object_type,
                 object_id,
                 action_type                                                                          as _action_type,
                 value,
                 timestamp,
                 datetime,
                 user_id = neighbor(user_id, 1) ? (neighbor(timestamp, 1, timestamp) - timestamp) : 0 as duration
          from (
                select user_id, object_type, object_id, action_type, value, timestamp, datetime
                from user_actions_buffer
                order by user_id, timestamp
                   )
          where action_type in [''mouseover'', ''mouseout'']
            and neighbor(action_type, 1) != '' click ''
            and neighbor(action_type
              , -1) != '' click ''
             )
    where _action_type = ''mouseover''
  and duration < hover_afk_timeout;


-- All batched actions
create view batched_actions as
    select *
    from clicks
    union all
    select *
    from hovers
    union all
    select *
    from scrolls;


-- Scroll intensity
select user_id, sum(value) / sum(duration) as scroll_intensity
from scrolls
group by user_id;

-- Average scroll speed

-- Percent of back scroll
with 5000 as const_scroll_jump_px
select user_id, sum(value < 0 ? -value : 0) / sum(value > 0 ? value : 0)
from scrolls
where abs(value) < const_scroll_jump_px
group by user_id;

-- Average image view time
select user_id, cast(avg(duration) as INT)
from clicks
group by user_id;

-- TODO посмотреть на изменение времени просмотра картинок

-- Total images viewed
select user_id, groupArray(object_id) as viewed_images, count()
from clicks
group by user_id;

-- Images viewed last week
select user_id, groupArray(object_id) as viewed_images, count()
from clicks
where dateDiff('' week '', datetime, now()) < 1
group by user_id;

-- Total time spent

-- Average time spent per week

-- Average session time

-- Average amount of sessions per week

-- Average images viewed per session
