with activity_weeks as (
select i::date as activity_week
-- change activity weeks here v
from generate_series('2017-02-27', '2017-04-04', '1 week'::interval) i),


premium_users as (
select *
from jt_dw.employers_dim ed
inner join public.user_subscriptions_history ush on ed.employer_id = ush.user_id
where 1=1
and name not like 'FREE_PREMIUM'
and country like 'UK'
and ed.city like 'London'
and (acquisition_channel like 'Organic' or acquisition_sub_channel like 'Organic' or acquisition_channel like 'Not available')
and registration_sub_channel like 'iOS'
and good_employer = 1
and employer_id not in (select user_id from public.user_balance)
and email not like '%@mfsa.%'
and email not like '%@jobtoday.com%'
and test_id in (1,2,5)
--and ed.employer_id not in 
--(select user_id from public.refs_devices_users rdu inner join public.devices d on rdu.device_id = d.id where d.type = 3)
--change cohort here v
and date_trunc ('week', registration_date) >= '2017-02-27'
),
--select * from premium_users

premium_jobs as (
select job_id, job_posted_date, jd.employer_id
from jt_dw.jobs_dim jd
inner join premium_users pu on jd.employer_id = pu.employer_id
where 1=1
and good_job = 1
),

pivot as (
select distinct date_trunc ('week', registration_date)::date as start_week,
activity_week,
(extract (year from activity_week) - extract (year from registration_date))*52 
+ extract (week from activity_week) - extract (week from registration_date) as weeks_after
from premium_users pu
cross join activity_weeks aw
),

registrations as (
select date_trunc ('week', registration_date)::date as cohort_week,
count (distinct employer_id) as registrations
from premium_users
group by 1),

published as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as published_week,
count (distinct pj.employer_id) as published_ep,
count (distinct job_id) as listings
from premium_users pu
cross join activity_weeks aw
inner join premium_jobs pj on pu.employer_id = pj.employer_id
where 1=1
and aw.activity_week =  date_trunc ('week', job_posted_date)
and aw.activity_week >= date_trunc ('week', registration_date)
group by 1,2
order by 1,2
),
--select * from published


chatting_pairs as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
date_trunc ('week', message_date)::date as chat_week,
ca.employer_id, jobseeker_id
from jt_dw.chats_activity ca
inner join premium_users pu on ca.employer_id = pu.employer_id
inner join premium_jobs pj on pu.employer_id = pj.employer_id
where employer_messages > 0
and date_trunc ('week', registration_date) = date_trunc ('week', job_posted_date)
order by 1,2
),
--select * from chatting_pairs

ep_messages as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
date_trunc ('week', message_date)::date as chat_week,
ca.employer_id, jobseeker_id, employer_messages
from jt_dw.chats_activity ca
inner join premium_users pu on ca.employer_id = pu.employer_id
inner join premium_jobs pj on pu.employer_id = pj.employer_id
where employer_messages > 0
and date_trunc ('week', registration_date) = date_trunc ('week', job_posted_date)
order by 1,2
),
--select * from ep_messages;

chats_sent as (
select distinct cohort_week,
chat_week,
sum (employer_messages) as ep_chats_sent
from ep_messages
group by 1,2
order by 1,2
),
--select * from chats_sent

chat_sessions as (
select distinct cohort_week,
chat_week,
count (*) as ep_chat_sessions
from chatting_pairs
group by 1,2
order by 1,2
),
--select * from chat_sessions

publ_ep_who_chat as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as activity_week,
count (distinct pu.employer_id) as publ_ep_who_chat
--ep_chat_sessions
--sum (employer_messages) as ep_chats_sent
from premium_users pu
inner join premium_jobs pj on pu.employer_id = pj.employer_id
inner join jt_dw.chats_activity ca on pu.employer_id = ca.employer_id
cross join activity_weeks aw
--left join chat_sessions cs on cs.cohort_week = date_trunc ('week', registration_date) and cs.chat_week = aw.activity_week
where 1=1 
and date_trunc ('week', registration_date) = date_trunc ('week', job_posted_date)
and date_trunc ('week', message_date) = activity_week
and aw.activity_week >= date_trunc ('week', registration_date)
and employer_messages > 0
group by 1,2
order by 1,2
),
--select * from chats_table

purchasing_eps as (
select *,
row_number() over (partition BY user_id ORDER BY created) as subscription_order
from premium_users pu
where name in ('MOBILE_PREMIUM_S', 'MOBILE_PREMIUM_M', 'MOBILE_PREMIUM_L')
),
--select * from purchasing_eps

totals as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as activity_week,
count (distinct employer_id) as purchasing_eps,
count (distinct subscription_id) as total_orders
--case when name like 'MOBILE_PREMIUM_S' then (count (distinct subscription_id)) end as small_premium_orders,
--case when name like 'MOBILE_PREMIUM_M' then (count (distinct subscription_id)) end as medium_premium_orders,
--case when name like 'MOBILE_PREMIUM_L' then (count (distinct subscription_id)) end as large_premium_orders
from purchasing_eps pe
cross join activity_weeks aw
where 1=1
and name in ('MOBILE_PREMIUM_S', 'MOBILE_PREMIUM_M', 'MOBILE_PREMIUM_L')
and aw.activity_week >= date_trunc ('week', registration_date)
and aw.activity_week = date_trunc ('week', created)
group by 1,2
order by 1,2
),
--select * from totals;

order_counts as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as activity_week,
--count (distinct employer_id) as purchasing_eps,
--count (distinct subscription_id) as total_orders,
case when name like 'MOBILE_PREMIUM_S' then (count (distinct subscription_id)) end as small_premium_orders,
case when name like 'MOBILE_PREMIUM_M' then (count (distinct subscription_id)) end as medium_premium_orders,
case when name like 'MOBILE_PREMIUM_L' then (count (distinct subscription_id)) end as large_premium_orders
from purchasing_eps pe
cross join activity_weeks aw
where 1=1
and name in ('MOBILE_PREMIUM_S', 'MOBILE_PREMIUM_M', 'MOBILE_PREMIUM_L')
and aw.activity_week >= date_trunc ('week', registration_date)
and aw.activity_week = date_trunc ('week', created)
group by 1,2, pe.name
order by 1,2
),
--select * from order_counts;

unique_purchasing_eps as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as activity_week,
count (distinct employer_id) as unique_purchasing_eps
from purchasing_eps pe
cross join activity_weeks aw
where 1=1
and subscription_order = 1
and name in ('MOBILE_PREMIUM_S', 'MOBILE_PREMIUM_M', 'MOBILE_PREMIUM_L')
and aw.activity_week >= date_trunc ('week', registration_date)
and aw.activity_week = date_trunc ('week', created)
group by 1,2
order by 1,2
),
--select * from unique_purchasing_eps

cum_unique_purchasing_eps as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as activity_week,
count (distinct employer_id) as cum_unique_purchasing_eps
from purchasing_eps pe
cross join activity_weeks aw
where 1=1
and subscription_order = 1
and name in ('MOBILE_PREMIUM_S', 'MOBILE_PREMIUM_M', 'MOBILE_PREMIUM_L')
and aw.activity_week >= date_trunc ('week', registration_date)
and aw.activity_week >= date_trunc ('week', created)
group by 1,2
order by 1,2
),
--select * from cum_unique_purchasing_eps

repeat_purchasing_eps as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as activity_week,
count (distinct employer_id) as repeat_purchasing_eps
from purchasing_eps pe
cross join activity_weeks aw
where 1=1
and subscription_order > 1
and name in ('MOBILE_PREMIUM_S', 'MOBILE_PREMIUM_M', 'MOBILE_PREMIUM_L')
and aw.activity_week >= date_trunc ('week', registration_date)
and aw.activity_week = date_trunc ('week', created)
group by 1,2
order by 1,2
),
--select * from repeat_purchasing_eps;

first_orders_count as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as activity_week,
case when name like 'MOBILE_PREMIUM_S' and subscription_order = 1 then (count (distinct subscription_id)) end as small_premium_first_orders,
case when name like 'MOBILE_PREMIUM_M' and subscription_order = 1 then (count (distinct subscription_id)) end as medium_premium_first_orders,
case when name like 'MOBILE_PREMIUM_L' and subscription_order = 1 then (count (distinct subscription_id)) end as large_premium_first_orders
from purchasing_eps pe
cross join activity_weeks aw
where 1=1
and name in ('MOBILE_PREMIUM_S', 'MOBILE_PREMIUM_M', 'MOBILE_PREMIUM_L')
and aw.activity_week >= date_trunc ('week', registration_date)
and aw.activity_week = date_trunc ('week', created)
group by 1,2, pe.name, subscription_order
),
--select * from first_orders

total_orders as (
select distinct oc.cohort_week as cohort_week,
oc.activity_week as activity_week,
--sum (purchasing_eps) as purchasing_eps,
--sum (total_orders) as total_orders,
sum (small_premium_orders) as small_premium_orders,
sum (medium_premium_orders) as medium_premium_orders,
sum (large_premium_orders) as large_premium_orders
from order_counts oc 
group by 1,2
order by 1,2
),
--select * from total_orders

first_orders as (
select distinct cohort_week,
activity_week,
sum (small_premium_first_orders) as small_premium_first_orders,
sum (medium_premium_first_orders) as medium_premium_first_orders,
sum (large_premium_first_orders) as large_premium_first_orders
from first_orders_count
group by 1,2
order by 1,2
),
--select * from first_orders

cum_unique_rep_purchasers as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as activity_week,
count (distinct employer_id) as cum_unique_rep_purchasers
from purchasing_eps
cross join activity_weeks aw
where 1=1 
and subscription_order > 1
and aw.activity_week >= date_trunc ('week', registration_date)
and date_trunc ('week', created) <= activity_week
group by 1,2
order by 1,2
)
--select * from cum_unique_rep_purchasers;


select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week,
weeks_after,
registrations,
published_ep,
listings,
publ_ep_who_chat,
ep_chat_sessions,
ep_chats_sent as ep_chat_messages_sent,
purchasing_eps,
unique_purchasing_eps,
--cum_unique_purchasing_eps,
total_orders as orders,
small_premium_orders,
medium_premium_orders,
large_premium_orders,
small_premium_first_orders,
medium_premium_first_orders,
large_premium_first_orders,
cum_unique_rep_purchasers
from premium_users pu
cross join activity_weeks aw
left join pivot on pivot.start_week = date_trunc ('week', registration_date) and pivot.activity_week = aw.activity_week
left join registrations r on r.cohort_week = date_trunc ('week', registration_date)
left join published p on p.cohort_week = date_trunc ('week', registration_date) and p.published_week = aw.activity_week
left join publ_ep_who_chat ct on ct.cohort_week = date_trunc ('week', registration_date) and ct.activity_week = aw.activity_week
left join total_orders on total_orders.cohort_week = date_trunc ('week', registration_date) and total_orders.activity_week = aw.activity_week
left join first_orders on first_orders.cohort_week = date_trunc ('week', registration_date) and first_orders.activity_week = aw.activity_week
left join chat_sessions cs on cs.cohort_week = date_trunc ('week', registration_date) and cs.chat_week = aw.activity_week
left join chats_sent on chats_sent.cohort_week = date_trunc ('week', registration_date) and chats_sent.chat_week = aw.activity_week
left join unique_purchasing_eps upe on upe.cohort_week = date_trunc ('week', registration_date) and upe.activity_week = aw.activity_week
left join cum_unique_rep_purchasers curp on curp.cohort_week = date_trunc ('week', registration_date) and curp.activity_week = aw.activity_week
left join totals on totals.cohort_week = date_trunc ('week', registration_date) and totals.activity_week = aw.activity_week
left join cum_unique_purchasing_eps cupe on cupe.cohort_week = date_trunc ('week', registration_date) and cupe.activity_week = aw.activity_week
where 1=1
and aw.activity_week >= date_trunc ('week', registration_date)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
order by 1,2,3,5

