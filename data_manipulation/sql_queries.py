class Query:

    initial = '''
        select user_id
        from user_actions_buffer
        group by user_id
    '''

    class Scroll:
        intensity = '''
            select user_id, sum(value) / sum(duration) as scroll_intensity
            from scrolls
            group by user_id
        '''

        avg_speed = '''
            select user_id, avg(scroll_amount) * 1000 / avg(session_duration) as avg_px_scrolled_per_second
            from (
                  select user_id, sum(value) as scroll_amount, max(timestamp) - min(timestamp) > 0 ? max(timestamp) - min(timestamp) : any(duration) as session_duration
                  from sessions
                  where action_type = 'scroll'
                  group by user_id, session_id
                     )
            group by user_id
        '''

        back_scroll_percent = '''
            with 5000 as const_scroll_jump_px
            select user_id, sum(value < 0 ? -value : 0) / sum(value > 0 ? value : 0) as back_scroll_fraction
            from scrolls
            where abs(value) < const_scroll_jump_px
            group by user_id
        '''

    class Time:
        total = '''
            select user_id, intDiv(sum(session_duration), 1000) as total_time_spent
            from (
                  select user_id, max(timestamp) - min(timestamp) as session_duration
                  from sessions
                  group by user_id, session_id
                     )
            group by user_id
        '''

        avg_image_view = '''
            select user_id, avg(duration) / 1000 as avg_image_view_time
            from clicks
            group by user_id
        '''

    class Images:
        viewed_images = '''
            select user_id, groupArray(object_id) as viewed_images, groupArray(duration) as duration
            from clicks
            group by user_id
        '''

        amount_of_viewed_images = '''
            select user_id, count() as amount_of_viewed_images
            from clicks
            group by user_id
        '''

        viewed_images_last_week = '''
            select user_id, groupArray(object_id) as viewed_images_last_week
            from clicks
            where dateDiff('week', datetime, now()) < 1
            group by user_id
        '''

        amount_of_viewed_images_last_week = '''
            select user_id, count() as amount_of_viewed_images_last_week
            from clicks
            where dateDiff('week', datetime, now()) < 1
            group by user_id
        '''

        avg_images_viewed_per_session = '''
            select user_id, avg(clicks_amount) as avg_views_per_session
            from (
                  select user_id, count() as clicks_amount
                  from sessions
                  where action_type = 'click'
                  group by user_id, session_id
                     )
            group by user_id
        '''

    class Sessions:
        total = '''
            select user_id, uniq(session_id) as sessions_amount
            from sessions
            group by user_id
        '''

        avg_session_time = '''
            select user_id, intDiv(avg(session_duration), 1000) as avg_session_duration
            from (
                  select user_id, max(timestamp) - min(timestamp) as session_duration
                  from sessions
                  group by user_id, session_id
                     )
            group by user_id
        '''

        avg_images_viewed_per_session = '''
            select user_id, avg(clicks_amount) as avg_views_amount_per_session
            from (
                  select user_id, count() as clicks_amount
                  from sessions
                  where action_type = 'click'
                  group by user_id, session_id
                     )
            group by user_id
        '''

        avg_sessions_per_week = '''
            select user_id, avg(sessions_amount) as avg_sessions_per_active_week
            from (
                  select user_id, count() as sessions_amount
                  from (
                        select user_id, toStartOfWeek(min(datetime)) as session_start_week
                        from sessions
                        group by user_id, session_id
                           )
                  group by user_id, session_start_week
                     )
            group by user_id
        '''
