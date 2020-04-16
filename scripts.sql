select *, datetime
from user_actions_buffer
where user_id = '127178ff-4fde-4fa0-af36-2b190a8ff188'
  and action_type = 'scroll'
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
select user_id,
       object_type,
       null                                                                                     as object_id,
       action_type,
       sum(_value)                                                                              as value,
       min(_timestamp)                                                                          as timestamp,
       arraySum(length(groupArray(_duration) as dur_arr) > 1 ? arrayPopBack(dur_arr) : dur_arr) as duration,
       group
from (
      select user_id,
             arrayPopBack(groupArray(object_type))            as _object_type,
             arrayPopBack(groupArray(object_id))              as _object_id,
             arrayPopBack(groupArray(action_type))            as _action_type,
             arrayPopBack(groupArray(value))                  as _value,
             arrayPopBack(groupArray(timestamp))              as _timestamp,
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
                   neighbor(timestamp, 1, 0) - timestamp        as duration,
                   duration <= const_scroll_gap_time_ms ? 0 : 1 as new_group
            from user_actions_buffer
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
     _duration as _duration,
     _group as group
group by user_id, group, object_type, action_type;


-- Batch clicks


select user_id,
       _d,
       thecumsum
from (
      select user_id,
             d,
             arrayCumSum(d) as cumsum
      from (
            select user_id, groupArray(data) as d
            from (select arrayJoin([1, 2]) as user_id, arrayJoin([1, 0, 0, 1, 0, 0, 0, 1, 0]) as data)
            group by user_id
               )
         )
    array Join
     cumsum AS thecumsum,
     d as _d;


-- Average scroll speed per second
select user_id, avg(value)
from (
      select user_id, sum(value) AS value, intDiv(timestamp, 1000) AS time_group
      from user_actions_buffer
      where action_type = 'scroll'
      group by user_id, time_group
         )
group by user_id;


-- List of hovered images
SELECT user_id,
       object_id,
       'hover'                               AS action_type,
       arrayFilter((x, i) -> (i % 2 = 0),
                   groupArray(runningDifference(data.timestamp)) AS dur_arr,
                   arrayEnumerate(dur_arr))  AS duration,
       arrayFilter((x, i) -> (i % 2 = 1),
                   groupArray(data.timestamp) AS time_arr,
                   arrayEnumerate(time_arr)) AS timestamp

FROM (
         SELECT user_id,
                object_id,
                timestamp
         FROM user_actions_buffer
         WHERE action_type IN ['mouseover', 'mouseout']
         ORDER BY user_id,
                  timestamp
         ) AS data
GROUP BY user_id, object_id
ORDER BY user_id;


-- List of interesting images
SELECT user_id,
       object_id,
       arrayFilter((x, i) -> (i > 0),
                   groupArray(runningDifference(data.timestamp)) AS dur_arr,
                   arrayEnumerate(dur_arr)) AS duration,
       groupArray(data.action_type)         AS action_type
FROM user_actions_buffer as data
WHERE data.action_type IN ['mouseover', 'mouseout', 'click']
GROUP BY user_id, object_id
ORDER BY user_id;


select number, neighbor(number, 1) - number
from system.numbers
limit 10