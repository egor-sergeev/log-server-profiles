select *, datetime
from user_actions_buffer
order by timestamp;

select uniq(user_id)
from user_actions_buffer;

select count()
from user_actions_buffer;


-- Table size
select concat(database, '.', table)                         AS table,
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
       null            as object_id,
       action_type,
       sum(_value)     as value,
       min(_timestamp) as timestamp,
       min(_datetime)  as datetime,
       sum(_duration)  as duration
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
                   user_id = neighbor(user_id, 1) ? (neighbor(timestamp, 1, timestamp) - timestamp) : 0 as duration,
                   duration <= const_scroll_gap_time_ms ? 0 : 1                                         as new_group
            from (
                  select user_id, object_type, object_id, action_type, value, timestamp, datetime
                  from user_actions_buffer
                  order by user_id, timestamp
                     )
            where action_type = 'scroll'
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
      where action_type = 'click'
         or neighbor(action_type, -2) = 'click' and neighbor(user_id, -2) = user_id
         )
where action_type = 'click'
  and duration > 0
  and duration < click_afk_timeout;


-- Batch hovers
create view hovers as
with 30 * 1000 as hover_afk_timeout
select user_id,
       object_type,
       object_id,
       CAST('hover',
            'Enum8(\'click\' = 1, \'mouseover\' = 2, \'mouseout\' = 3, \'scroll\' = 4, \'hover\' = 5)') as action_type,
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
      where action_type in ['mouseover', 'mouseout']
        and neighbor(action_type, 1) != 'click'
        and neighbor(action_type, -1) != 'click'
         )
where _action_type = 'mouseover'
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


-- User sessions
create view sessions as
select user_id,
       object_type,
       _object_id = CAST('00000000-0000-0000-0000-000000000000' as UUID) ? null : _object_id as object_id,
       action_type,
       _value = -9223372036854775808 ? null : _value                                         as value,
       timestamp,
       datetime,
       duration,
       session_id
from (
      select user_id,
             arrayPopBack(groupArray(object_type))            as _object_type,
             arrayPopBack(groupArray(object_id))              as _object_id,
             arrayPopBack(groupArray(action_type))            as _action_type,
             arrayPopBack(groupArray(value))                  as _value,
             arrayPopBack(groupArray(timestamp))              as _timestamp,
             arrayPopBack(groupArray(datetime))               as _datetime,
             arrayPopBack(groupArray(duration))               as _duration,
             arrayPopBack(arrayCumSum(groupArray(new_group))) as _session
      from (
            with 10 * 60 * 1000 as const_session_gap_time_ms
            select user_id,
                   object_type,
                   object_id is null ? CAST('00000000-0000-0000-0000-000000000000' as UUID) : object_id as object_id,
                   action_type,
                   value is null ? -9223372036854775808 : value                                         as value,
                   timestamp,
                   datetime,
                   duration,
                   timestamp - neighbor(timestamp, -1, timestamp) <= const_session_gap_time_ms ? 0 : 1  as new_group
            from (
                  select user_id,
                         object_type,
                         object_id,
                         action_type,
                         value,
                         timestamp,
                         duration,
                         datetime
                  from batched_actions
                  order by user_id, timestamp
                     )
            order by user_id, timestamp
               )
      group by user_id
         )
    array join
     _object_type as object_type,
    _object_id,
     _action_type as action_type,
    _value,
     _timestamp as timestamp,
     _datetime as datetime,
     _duration as duration,
     _session as session_id;


-- Scroll intensity
select user_id, sum(value) / sum(duration) as scroll_intensity
from scrolls
group by user_id;


-- Average scroll speed
select user_id, avg(scroll_amount) * 1000 / avg(session_duration) as avg_px_scrolled_per_second
from (
      select user_id, sum(value) as scroll_amount, max(timestamp) - min(timestamp) as session_duration
      from sessions
      where action_type = 'scroll'
      group by user_id, session_id
         )
group by user_id;


-- Percent of back scroll
with 5000 as const_scroll_jump_px
select user_id, sum(value < 0 ? -value : 0) / sum(value > 0 ? value : 0) as back_scroll_fraction
from scrolls
where abs(value) < const_scroll_jump_px
group by user_id;


-- Average image view time
select user_id, avg(duration) / 1000 as avg_image_view_time
from clicks
group by user_id;


-- Total images viewed
select user_id, groupArray(object_id) as viewed_images, count() as amount
from clicks
group by user_id;


-- Images viewed last week
select user_id, groupArray(object_id) as viewed_images_last_week, count() as amount
from clicks
where dateDiff('week', datetime, now()) < 1
group by user_id;


-- Total time spent
select user_id, intDiv(sum(session_duration), 1000) as total_time_spent
from (
      select user_id, max(timestamp) - min(timestamp) as session_duration
      from sessions
      group by user_id, session_id
         )
group by user_id;


-- Average time spent per week
select user_id, min(timestamp) as session_start
from sessions
group by user_id, session_id;

-- SESSIONS --
-- Total sessions
select user_id, uniq(session_id) as sessions_amount
from sessions
group by user_id;

-- Average session time
select user_id, count() as sessions_amount, intDiv(avg(session_duration), 1000) as avg_session_duration
from (
      select user_id, max(timestamp) - min(timestamp) as session_duration
      from sessions
      group by user_id, session_id
         )
group by user_id;

-- Average images viewed per session
select user_id, avg(clicks_amount) as avg_clicks_amount
from (
      select user_id, count() as clicks_amount
      from sessions
      where action_type = 'click'
      group by user_id, session_id
         )
group by user_id;

-- Average sessions per week
select user_id, avg(sessions_amount) as avg_sessions_per_week
from (
      select user_id, count() as sessions_amount
      from (
            select user_id, toStartOfWeek(min(datetime)) as session_start_week
            from sessions
            group by user_id, session_id
               )
      group by user_id, session_start_week
         )
group by user_id;
