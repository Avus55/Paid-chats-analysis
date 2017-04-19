with activity_weeks as (
select i::date as activity_week
-- change activity weeks here v
from generate_series('2016-06-06', '2016-09-19', '1 week'::interval) i),


free_users as (
select * from jt_dw.employers_dim
where 1=1
and country like 'UK'
and city like 'London'
and (acquisition_channel like 'Organic' or acquisition_sub_channel like 'Organic' or acquisition_channel like 'Not available')
and registration_sub_channel like 'iOS'
and good_employer = 1
and email not like '%@mfsa.%'
and email not like '%@jobtoday.com%'
--change cohort here v
and date_trunc ('week', registration_date) between '2016-06-06' and '2016-07-25'
),
--select * from free_users

pivot as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
activity_week,
(extract (year from activity_week) - extract (year from registration_date))*52 
+ extract (week from activity_week) - extract (week from registration_date) as weeks_after
from free_users fu
cross join activity_weeks aw
),

registrations as (
select date_trunc ('week', registration_date)::date as cohort_week,
count (distinct employer_id) as registrations
from free_users
group by 1
),

free_jobs as (
select job_id, job_posted_date, jd.employer_id
from jt_dw.jobs_dim jd
inner join free_users fu on jd.employer_id = fu.employer_id
where 1=1
and good_job = 1
),

published as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as activity_week,
count (distinct fj.employer_id) as published_eps,
count (distinct job_id) as listings
from free_users fu
cross join activity_weeks aw
inner join free_jobs fj on fu.employer_id = fj.employer_id
where 1=1
and aw.activity_week =  date_trunc ('week', job_posted_date)
and aw.activity_week >= date_trunc ('week', registration_date)
group by 1,2
order by 1,2
),
--select * from published

publ_eps_who_chat as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as activity_week,
count (distinct ca.employer_id) as publ_eps_who_chat
from free_users fu
inner join free_jobs fj on fu.employer_id = fj.employer_id
inner join jt_dw.chats_activity ca on ca.employer_id = fj.employer_id
cross join activity_weeks aw
where 1=1
and aw.activity_week = date_trunc ('week', message_date)
and aw.activity_week >= date_trunc ('week', registration_date)
and date_trunc ('week', registration_date) = date_trunc ('week', job_posted_date)
and employer_messages > 0
group by 1,2
order by 1,2
), 
--select * from publ_eps_who_chat

chatting_pairs as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as activity_week,
ca.employer_id, jobseeker_id
from free_users fu
inner join jt_dw.chats_activity ca on ca.employer_id = fu.employer_id
inner join free_jobs fj on fj.employer_id = fu.employer_id
cross join activity_weeks aw
where 1=1
and employer_messages > 0
and date_trunc ('week', message_date) = activity_week
and date_trunc ('week', registration_date) = date_trunc ('week', job_posted_date)
order by 1,2
),
--select * from chatting_pairs;

ep_chat_sessions as (
select distinct cohort_week, activity_week,
count (*) as ep_chat_sessions
from chatting_pairs
group by 1,2
order by 1,2
),
--select * from ep_chat_sessions;

ep_messages as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as activity_week,
ca.employer_id, jobseeker_id, employer_messages
from free_users fu
inner join jt_dw.chats_activity ca on ca.employer_id = fu.employer_id
inner join free_jobs fj on fj.employer_id = fu.employer_id
cross join activity_weeks aw
where 1=1
and employer_messages > 0
and date_trunc ('week', message_date) = activity_week
and date_trunc ('week', registration_date) = date_trunc ('week', job_posted_date)
order by 1,2
),
--select * from ep_messages;

ep_chats_sent as (
select distinct cohort_week, activity_week,
sum (employer_messages) as ep_chats_sent
from ep_messages
group by 1,2
order by 1,2
)
--select * from ep_chats_sent;


select distinct date_trunc ('week', fu.registration_date)::date as cohort_week,
aw.activity_week,
weeks_after,
registrations,
published_eps,
listings,
publ_eps_who_chat,
ep_chat_sessions,
ep_chats_sent
from free_users fu
cross join activity_weeks aw
left join pivot on pivot.cohort_week = date_trunc ('week', fu.registration_date) and pivot.activity_week = aw.activity_week
left join registrations reg on reg.cohort_week = date_trunc ('week', fu.registration_date)
left join published pub on pub.cohort_week = date_trunc ('week', fu.registration_date) and pub.activity_week = aw.activity_week
left join publ_eps_who_chat pewc on pewc.cohort_week = date_trunc ('week', fu.registration_date) and pewc.activity_week = aw.activity_week
left join ep_chat_sessions ecs on ecs.cohort_week = date_trunc ('week', fu.registration_date) and ecs.activity_week = aw.activity_week
left join ep_chats_sent on ep_chats_sent.cohort_week = date_trunc ('week', fu.registration_date) and ep_chats_sent.activity_week = aw.activity_week
where 1=1
and aw.activity_week >= date_trunc ('week', fu.registration_date)
order by 1,2


