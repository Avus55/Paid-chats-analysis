with activity_weeks as (
select i::date as activity_week
-- change activity weeks here v
from generate_series('2017-02-27', '2017-04-03', '1 week'::interval) i),


paid_chats_users as (
select * from jt_dw.employers_dim ed
inner join public.user_balance ub on ed.employer_id = ub.user_id
inner join public.users u on u.id = ed.employer_id
where 1=1
and country like 'UK'
and ed.city like 'London'
and (acquisition_channel like 'Organic' or acquisition_sub_channel like 'Organic' or acquisition_channel like 'Not available')
and registration_sub_channel like 'iOS'
and good_employer = 1
and email not like '%@mfsa.%'
and email not like '%@jobtoday.com%'
--change cohort here v
and date_trunc ('week', registration_date) >= '2017-02-27'
),
--select * from paid_chats_users

premium_jobs as (
select job_id, job_posted_date, jd.employer_id
from jt_dw.jobs_dim jd
inner join paid_chats_users pcu on jd.employer_id = pcu.employer_id
where 1=1
and good_job = 1
),

pivot as (
select distinct date_trunc ('week', registration_date)::date as start_week,
activity_week,
(extract (year from activity_week) - extract (year from registration_date))*52 
+ extract (week from activity_week) - extract (week from registration_date) as weeks_after
from paid_chats_users pcu
cross join activity_weeks aw
),

registrations as (
select date_trunc ('week', registration_date)::date as cohort_week,
count (distinct employer_id) as registrations
from paid_chats_users pcu
group by 1),

published as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as published_week,
count (distinct pj.employer_id) as published_ep,
count (distinct job_id) as listings
from paid_chats_users pcu
inner join premium_jobs pj on pcu.employer_id = pj.employer_id
cross join activity_weeks aw
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
inner join paid_chats_users pcu on ca.employer_id = pcu.employer_id
inner join premium_jobs pj on pcu.employer_id = pj.employer_id
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
inner join paid_chats_users pcu on ca.employer_id = pcu.employer_id
inner join premium_jobs pj on pcu.employer_id = pj.employer_id
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
from 
(select distinct * from chatting_pairs) as foo
group by 1,2
order by 1,2
),
--select * from chat_sessions

publ_ep_who_chat as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as activity_week,
count (distinct pcu.employer_id) as publ_ep_who_chat
--ep_chat_sessions,
--sum (employer_messages) as ep_chats_sent
from paid_chats_users pcu
inner join premium_jobs pj on pcu.employer_id = pj.employer_id
inner join jt_dw.chats_activity ca on pcu.employer_id = ca.employer_id
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
--select * from publ_ep_who_chat;

cum_publ_ep_who_chat as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as activity_week,
count (distinct pcu.employer_id) as cum_publ_ep_who_chat
--ep_chat_sessions,
--sum (employer_messages) as ep_chats_sent
from paid_chats_users pcu
inner join premium_jobs pj on pcu.employer_id = pj.employer_id
inner join jt_dw.chats_activity ca on pcu.employer_id = ca.employer_id
cross join activity_weeks aw
--left join chat_sessions cs on cs.cohort_week = date_trunc ('week', registration_date) and cs.chat_week = aw.activity_week
where 1=1 
and date_trunc ('week', registration_date) = date_trunc ('week', job_posted_date)
and date_trunc ('week', message_date) <= activity_week
and aw.activity_week >= date_trunc ('week', registration_date)
and employer_messages > 0
group by 1,2
order by 1,2
),
--select * from cum_publ_ep_who_chat;

purchasing_eps as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as activity_week,
count (distinct pay.user_id) as purchasing_eps,
count (distinct pay.id) as total_orders
--case when pur.purchase_id like 'com.jobtodayapp.jobiki.pack1%' then (count (distinct pay.id)) end as pack_1_orders,
--case when pur.purchase_id like 'com.jobtodayapp.jobiki.pack2%' then (count (distinct pay.id)) end as pack_2_orders,
--case when pur.purchase_id like 'com.jobtodayapp.jobiki.pack3%' then (count (distinct pay.id)) end as pack_3_orders
from paid_chats_users pcu
inner join public.payment pay on pcu.employer_id = pay.user_id
inner join public.purchases pur on pay.purchase_id = pur.id
cross join activity_weeks aw
where 1=1
and pur.purchase_id like 'com.jobtodayapp.jobiki%'
and aw.activity_week >= date_trunc ('week', registration_date)
and aw.activity_week = date_trunc ('week', pay.created)
group by 1,2
order by 1,2
),
--select * from purchasing_eps;

pack_orders as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as activity_week,
--count (distinct pay.user_id) as purchasing_eps,
--count (distinct pay.id) as total_orders,
case when pur.purchase_id like 'com.jobtodayapp.jobiki.pack1%' then (count (distinct pay.id)) end as pack_1_orders,
case when pur.purchase_id like 'com.jobtodayapp.jobiki.pack2%' then (count (distinct pay.id)) end as pack_2_orders,
case when pur.purchase_id like 'com.jobtodayapp.jobiki.pack3%' then (count (distinct pay.id)) end as pack_3_orders
from paid_chats_users pcu
inner join public.payment pay on pcu.employer_id = pay.user_id
inner join public.purchases pur on pay.purchase_id = pur.id
cross join activity_weeks aw
where 1=1
and pur.purchase_id like 'com.jobtodayapp.jobiki%'
and aw.activity_week >= date_trunc ('week', registration_date)
and aw.activity_week = date_trunc ('week', pay.created)
group by 1,2, pur.purchase_id
order by 1,2
),
--select * from pack_orders;


chats_payments as (
select *,
row_number () over (partition by user_id order by created) as pur_order
from (
select distinct pay.user_id, 
date_trunc ('week', pcu.registration_date)::date as registration_week,
date_trunc ('week', pay.created) as created, 
pay.purchase_id, pay.id
--row_number () over (partition by pay.user_id) as pur_order
from public.payment pay
inner join public.purchases pur on pay.purchase_id = pur.id
inner join paid_chats_users pcu on pcu.employer_id = pay.user_id
where 1=1
and pur.purchase_id like 'com.jobtodayapp.jobiki%'
order by 2,3,1) as foo
),
--select * from chats_payments;

unique_purchasing_eps as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as activity_week,
count (distinct cp.user_id) as unique_purchasing_eps
from paid_chats_users pcu
inner join chats_payments cp on pcu.employer_id = cp.user_id
cross join activity_weeks aw
where 1=1
and pur_order = 1
and aw.activity_week >= date_trunc ('week', registration_date)
and aw.activity_week = date_trunc ('week', cp.created)
group by 1,2
order by 1,2
),
--select * from unique_purchasing_eps;

cum_unique_purchasing_eps as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as activity_week,
count (distinct cp.user_id) as cum_unique_purchasing_eps
from paid_chats_users pcu
inner join chats_payments cp on pcu.employer_id = cp.user_id
cross join activity_weeks aw
where 1=1
--and pur_order = 1
and aw.activity_week >= date_trunc ('week', registration_date)
and aw.activity_week >= date_trunc ('week', cp.created)
group by 1,2
order by 1,2
),
--select * from cum_unique_purchasing_eps;

first_and_repeat as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as activity_week,
case when (pur.purchase_id like 'com.jobtodayapp.jobiki.pack1%' and pur_order = 1) then (count (distinct cp.id)) end as first_orders_pack_1,
case when (pur.purchase_id like 'com.jobtodayapp.jobiki.pack2%' and pur_order = 1) then (count (distinct cp.id)) end as first_orders_pack_2,
case when (pur.purchase_id like 'com.jobtodayapp.jobiki.pack3%' and pur_order = 1) then (count (distinct cp.id)) end as first_orders_pack_3
--case when (pur.purchase_id like 'com.jobtodayapp.jobiki.pack1%' and pur_order > 1) then (count (distinct cp.id)) end as repeat_orders_pack_1,
--case when (pur.purchase_id like 'com.jobtodayapp.jobiki.pack2%' and pur_order > 1) then (count (distinct cp.id)) end as repeat_orders_pack_2,
--case when (pur.purchase_id like 'com.jobtodayapp.jobiki.pack3%' and pur_order > 1) then (count (distinct cp.id)) end as repeat_orders_pack_3
from paid_chats_users pcu
inner join chats_payments cp on pcu.employer_id = cp.user_id
inner join public.purchases pur on cp.purchase_id = pur.id
cross join activity_weeks aw
where 1=1
and aw.activity_week >= date_trunc ('week', registration_date)
and aw.activity_week = date_trunc ('week', cp.created)
group by 1,2, pur.purchase_id, cp.pur_order
),
--select * from first_orders

total_orders as (
select distinct cohort_week as cohort_week,
activity_week as activity_week,
--sum (purchasing_eps) as purchasing_eps,
--sum (total_orders) as total_orders,
sum (pack_1_orders) as total_pack_1_orders,
sum (pack_2_orders) as total_pack_2_orders,
sum (pack_3_orders) as total_pack_3_orders
from pack_orders
group by 1,2
),
--select * from total_orders

first_orders as (
select distinct  cohort_week as cohort_week,
activity_week as activity_week,
sum (first_orders_pack_1) as first_orders_pack_1,
sum (first_orders_pack_2) as first_orders_pack_2,
sum(first_orders_pack_3) as first_orders_pack_3
--sum (repeat_orders_pack_1) as repeat_orders_pack_1,
--sum (repeat_orders_pack_2) as repeat_orders_pack_2,
--sum (repeat_orders_pack_3) as repeat_orders_pack_3
from first_and_repeat
group by 1,2
),
--select * from first_orders

cum_unique_rep_purchasers as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as activity_week,
count (distinct employer_id) as cum_unique_rep_purchasers
from paid_chats_users pcu
inner join chats_payments cp on pcu.employer_id = cp.user_id
cross join activity_weeks aw
where 1=1 
and pur_order > 1
and aw.activity_week >= date_trunc ('week', registration_date)
and date_trunc ('week', cp.created) <= activity_week
group by 1,2
order by 1,2
),
--select * from cum_unique_rep_purchasers;

balance_history as (
select *,
sum (balance_change) over (partition by user_id order by created) as balance
from public.user_balance_history
group by 2,1
order by 2,4
),
--select * from balance_history

eps_with_0_credits as (
select *
from paid_chats_users pcu
inner join balance_history bh on pcu.employer_id = bh.user_id
where 1=1
and bh.balance = 0
--group by 1,2
order by 1,2
),
--select * from eps_with_0_credits;

cum_eps_who_reached_0_credits as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as activity_week,
count (distinct employer_id) as cum_eps_who_reached_0_credits
from eps_with_0_credits ew0c
cross join activity_weeks aw
where 1=1
and aw.activity_week >= date_trunc ('week', registration_date)
and aw.activity_week >= date_trunc ('week', created)
group by 1,2
order by 1,2
),
--select * from cum_eps_who_reached_0_credits;

chats_payments_0_credits as (
select distinct pay.user_id, 
date_trunc ('week', ew0c.registration_date)::date as registration_week,
date_trunc ('week', pay.created) as created, 
pay.purchase_id, pay.id,
row_number () over (partition by pay.user_id) as pur_order
from public.payment pay
inner join public.purchases pur on pay.purchase_id = pur.id
inner join eps_with_0_credits ew0c on ew0c.employer_id = pay.user_id
where 1=1
and pur.purchase_id like 'com.jobtodayapp.jobiki%'
order by 2,3,1
),

cum_purchasing_eps_who_reached_0_credits as (
select distinct date_trunc ('week', registration_date)::date as cohort_week,
aw.activity_week as activity_week,
count (distinct cp0c.user_id) as cum_purchasing_eps_who_reached_0_credits
from eps_with_0_credits ew0c
inner join chats_payments_0_credits cp0c on ew0c.employer_id = cp0c.user_id
cross join activity_weeks aw
where 1=1
and pur_order = 1
and aw.activity_week >= date_trunc ('week', registration_date)
and aw.activity_week >= date_trunc ('week', cp0c.created)
and aw.activity_week >= date_trunc ('week', ew0c.created)
group by 1,2
order by 1,2
)
--select * from cum_purchasing_eps_who_reached_0_credits;


select distinct date_trunc ('week', pcu.registration_date)::date as cohort_week,
aw.activity_week,
weeks_after,
registrations,
published_ep,
listings,
publ_ep_who_chat,
cum_publ_ep_who_chat,
ep_chat_sessions,
ep_chats_sent as ep_chat_messages_sent,
cum_eps_who_reached_0_credits,
--cum_purchasing_eps_who_reached_0_credits,
pe.purchasing_eps,
unique_purchasing_eps,
--cum_unique_purchasing_eps,
pe.total_orders as orders,
total_orders.total_pack_1_orders as pack_1_orders,
total_orders.total_pack_2_orders as pack_2_orders,
total_orders.total_pack_3_orders as pack_3_orders,
first_orders_pack_1,
first_orders_pack_2,
first_orders_pack_3,
--repeat_orders_pack_1,
--repeat_orders_pack_2,
--repeat_orders_pack_3
cum_unique_rep_purchasers
from paid_chats_users pcu
cross join activity_weeks aw
left join pivot on pivot.start_week = date_trunc ('week', pcu.registration_date) and pivot.activity_week = aw.activity_week
left join registrations r on r.cohort_week = date_trunc ('week', pcu.registration_date)
left join published p on p.cohort_week = date_trunc ('week', pcu.registration_date) and p.published_week = aw.activity_week
left join publ_ep_who_chat ct on ct.cohort_week = date_trunc ('week', pcu.registration_date) and ct.activity_week = aw.activity_week
left join total_orders on total_orders.cohort_week = date_trunc ('week', pcu.registration_date) and total_orders.activity_week = aw.activity_week
left join first_orders on first_orders.cohort_week = date_trunc ('week', pcu.registration_date) and first_orders.activity_week = aw.activity_week
left join chat_sessions cs on cs.cohort_week = date_trunc ('week', registration_date) and cs.chat_week = aw.activity_week
left join chats_sent on chats_sent.cohort_week = date_trunc ('week', registration_date) and chats_sent.chat_week = aw.activity_week
left join cum_unique_purchasing_eps cupe on cupe.cohort_week = date_trunc ('week', registration_date) and cupe.activity_week = aw.activity_week
left join cum_unique_rep_purchasers curp on curp.cohort_week = date_trunc ('week', registration_date) and curp.activity_week = aw.activity_week
left join purchasing_eps pe on pe.cohort_week = date_trunc ('week', registration_date) and pe.activity_week = aw.activity_week
left join unique_purchasing_eps upe on upe.cohort_week = date_trunc ('week', registration_date) and upe.activity_week = aw.activity_week
left join cum_eps_who_reached_0_credits cewr0c on cewr0c.cohort_week = date_trunc ('week', registration_date) and cewr0c.activity_week = aw.activity_week
left join cum_purchasing_eps_who_reached_0_credits cpewr0c on cpewr0c.cohort_week = date_trunc ('week', registration_date) and cpewr0c.activity_week = aw.activity_week
left join cum_publ_ep_who_chat cpewc on cpewc.cohort_week = date_trunc ('week', registration_date) and cpewc.activity_week = aw.activity_week
where 1=1
and aw.activity_week >= date_trunc ('week', pcu.registration_date)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
order by 1,2,3;
