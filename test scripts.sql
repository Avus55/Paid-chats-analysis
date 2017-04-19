with premium_users as (
select *
from jt_dw.employers_dim ed
inner join public.user_subscriptions_history ush on ed.employer_id = ush.user_id
where 1=1
and name not like 'FREE_PREMIUM'
and country like 'UK'
and city like 'London'
and (acquisition_channel like 'Organic' or acquisition_sub_channel like 'Organic' or acquisition_channel like 'Not available')
and registration_sub_channel like 'iOS'
and good_employer = 1
and employer_id not in (select user_id from public.user_balance)
--change cohort here v
and date_trunc ('week', registration_date) = '2017-02-20'
)

select distinct
count (distinct employer_id) 
--employer_id
from premium_users
where date_trunc ('week', registration_date) = '2017-02-20';



with paid_chats_users as (
select * from jt_dw.employers_dim ed
inner join public.user_balance ub on ed.employer_id = ub.user_id
where 1=1
and country like 'UK'
--and city like 'London'
--and (acquisition_channel like 'Organic' or acquisition_sub_channel like 'Organic' or acquisition_channel like 'Not available')
and registration_sub_channel like 'iOS'
--and good_employer = 1
--change cohort here v
and registration_date >= '2017-02-06'
)

select 
--count (distinct employer_id) from paid_chats_users
* from paid_chats_users
where date_trunc ('week', registration_date) <= '2017-03-06'
and email like '%@mfsa.%';



select distinct *,
row_number () over (partition by pay.user_id) as pur_order
from public.payment pay
inner join public.purchases pur on pay.purchase_id = pur.id
inner join jt_dw.employers_dim ed on ed.employer_id = pay.user_id
where 1=1
and pur.purchase_id like 'com.jobtodayapp.jobiki%'
and email like '%@mfsa.%';


--trying to find out chat ratio for published EPs
with published_eps as (
select *,
ed.employer_id as emp_id
from jt_dw.employers_dim ed
inner join jt_dw.jobs_dim jd on ed.employer_id = jd.employer_id
where 1=1
and country like 'UK'
and good_job = 1
and good_employer = 1
and date_trunc ('week', registration_date) = '2017-03-06'
and date_trunc ('week', job_posted_date) = '2017-03-06'
and city like 'London'
and registration_sub_channel like 'iOS'
and acquisition_channel in ('Organic', 'Not available')
)
--select count (distinct emp_id) from published_eps;

select count (distinct emp_id)
from published_eps pe
inner join jt_dw.chats_activity ca on pe.emp_id = ca.employer_id
where 1=1
and date_trunc ('week', message_date) = '2017-03-06'
and employer_messages > 0
;


select *,
sum (balance_change) over (partition by user_id order by created) as balance
from public.user_balance_history
group by 2,1
order by 2,4;

---purchasing paid chats users
with paid_chats_users as (
select * from jt_dw.employers_dim ed
inner join public.user_balance ub on ed.employer_id = ub.user_id
where 1=1
and country like 'ES'
--and city like 'London'
--and (acquisition_channel like 'Organic' or acquisition_sub_channel like 'Organic' or acquisition_channel like 'Not available')
--and registration_sub_channel like 'iOS'
--and good_employer = 1
and email not like '%@mfsa.%'
and email not like '%@jobtoday.com%'
--change cohort here v
and date_trunc ('week', registration_date) >= '2017-02-06'
)

select distinct employer_id, pur.purchase_id
from paid_chats_users pcu
inner join public.payment pay on pcu.employer_id = pay.user_id
inner join public.purchases pur on pay.purchase_id = pur.id
where 1=1
and pur.purchase_id like 'com.jobtodayapp.jobiki%'
and (pur.purchase_id like '%3%' or pur.purchase_id like '%2%');





