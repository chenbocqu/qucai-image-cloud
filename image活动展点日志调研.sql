
SELECT 
  t, dt, SUM(1) as cnt , SUM(if(trim(nvl(p1, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as p1, SUM(if(trim(nvl(c1, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as c1, SUM(if(trim(nvl(s2, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as s2, SUM(if(trim(nvl(s3, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as s3, SUM(if(trim(nvl(u, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as u, SUM(if(trim(nvl(pu, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as pu, SUM(if(trim(nvl(v, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as v, SUM(if(trim(nvl(br, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as br, SUM(if(trim(nvl(rn, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as rn, SUM(if(trim(nvl(nu, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as nu, SUM(if(trim(nvl(ce, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as ce, SUM(if(trim(nvl(de, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as de, SUM(if(trim(nvl(hu, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as hu, SUM(if(trim(nvl(rfr, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as rfr, SUM(if(trim(nvl(bstp, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as bstp, SUM(if(trim(nvl(mkey, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as mkey, SUM(if(trim(nvl(stime, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as stime, SUM(if(trim(nvl(mod, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as mod, SUM(if(trim(nvl(ua_model, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as ua_model, SUM(if(trim(nvl(lrfr, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as lrfr, SUM(if(trim(nvl(as, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as as, SUM(if(trim(nvl(t, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as t, SUM(if(trim(nvl(rseat, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as rseat, SUM(if(trim(nvl(block, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as block, SUM(if(trim(nvl(position, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as position, SUM(if(trim(nvl(rpage, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as rpage, SUM(if(trim(nvl(purl, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as purl, SUM(if(trim(nvl(s4, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as s4, SUM(if(trim(nvl(p2, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as p2, SUM(if(trim(nvl(dfp, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as dfp, SUM(if(trim(nvl(dm, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as dm, SUM(if(trim(nvl(ip, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as ip, SUM(if(trim(nvl(nation_id, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as nation_id, SUM(if(trim(nvl(region_id, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as region_id, SUM(if(trim(nvl(province_id, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as province_id, SUM(if(trim(nvl(city_id, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as city_id, SUM(if(trim(nvl(isp_id, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as isp_id, SUM(if(trim(nvl(ua, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as ua
FROM 
  bi_warehouse_ods.bi_warehouse_ods__ods_subject_action_cxidnotnull a
WHERE 
  dt BETWEEN '2020-07-05' AND '2020-07-13'
GROUP BY t, dt 
ORDER BY t, dt

-- 新增4个字段
SELECT 
    dt, SUM(1) as cnt , SUM(if(trim(nvl(p1, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as p1, SUM(if(trim(nvl(c1, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as c1, SUM(if(trim(nvl(s2, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as s2, SUM(if(trim(nvl(s3, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as s3, SUM(if(trim(nvl(u, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as u, SUM(if(trim(nvl(pu, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as pu, SUM(if(trim(nvl(v, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as v, SUM(if(trim(nvl(br, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as br, SUM(if(trim(nvl(rn, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as rn, SUM(if(trim(nvl(nu, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as nu, SUM(if(trim(nvl(ce, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as ce, SUM(if(trim(nvl(de, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as de, SUM(if(trim(nvl(hu, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as hu, SUM(if(trim(nvl(rfr, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as rfr, SUM(if(trim(nvl(bstp, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as bstp, SUM(if(trim(nvl(mkey, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as mkey, SUM(if(trim(nvl(stime, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as stime, SUM(if(trim(nvl(mod, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as mod, SUM(if(trim(nvl(ua_model, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as ua_model, SUM(if(trim(nvl(lrfr, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as lrfr, SUM(if(trim(nvl(as, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as as, SUM(if(trim(nvl(t, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as t, SUM(if(trim(nvl(rseat, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as rseat, SUM(if(trim(nvl(block, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as block, SUM(if(trim(nvl(position, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as position, SUM(if(trim(nvl(rpage, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as rpage, SUM(if(trim(nvl(purl, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as purl, SUM(if(trim(nvl(s4, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as s4, SUM(if(trim(nvl(p2, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as p2, SUM(if(trim(nvl(dfp, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as dfp, SUM(if(trim(nvl(dm, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as dm, SUM(if(trim(nvl(ip, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as ip, SUM(if(trim(nvl(nation_id, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as nation_id, SUM(if(trim(nvl(region_id, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as region_id, SUM(if(trim(nvl(province_id, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as province_id, SUM(if(trim(nvl(city_id, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as city_id, SUM(if(trim(nvl(isp_id, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as isp_id, SUM(if(trim(nvl(ua, '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as ua, SUM(if(trim(nvl(custom_cols['cxid'], '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as cxid,


    SUM(if(trim(nvl(custom_cols['model'], '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as model,
    SUM(if(trim(nvl(custom_cols['wifimac'], '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as wifimac,
    SUM(if(trim(nvl(custom_cols['ntwk'], '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as ntwk,
    SUM(if(trim(nvl(custom_cols['mac_address'], '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as mac_address,
    SUM(if(trim(nvl(custom_cols['gps'], '')) in (0,'','NULL','null'), 1, 0))/SUM(1) as gps

FROM 
    bi_warehouse_ods.bi_warehouse_ods__ods_subject_action_cxidnotnull a
WHERE 
    dt BETWEEN '2020-07-05' AND '2020-07-13' AND t IN (20, 21)
GROUP BY dt
ORDER BY dt

-- custom_cols cxid、ua 调研

select
  -- get_json_object( custom_cols,'$.cxid')
  custom_cols['gps'], sum(1) cnt
from
  bi_warehouse_ods.bi_warehouse_ods__ods_subject_action_cxidnotnull
where dt='2020-07-12' AND t IN (20, 21)
group by custom_cols['gps']
order by cnt desc
limit 500

select
  custom_cols
from
  bi_warehouse_ods.bi_warehouse_ods__ods_subject_action_cxidnotnull
where dt='2020-07-12'
limit 10;


select
  -- get_json_object( custom_cols,'$.cxid')
  t,  sum(1) cnt
from
  bi_warehouse_ods.bi_warehouse_ods__ods_subject_action_cxidnotnull
where dt='2020-07-12' AND custom_cols['cxid']=0
group by t
order by cnt desc
limit 500


use antispam_dwd;

set mapred.job.name=p0@V_MHAPP@DAY@IPHONE_START_FILTER_${DT}@step0;
set hive.exec.reducers.bytes.per.reducer=256000000;
set hive.exec.reducers.max=500;
set hive.auto.convert.join=false;
set hive.optimize.reducededuplication=true;
set hive.exec.parallel.thread.number=10;
set mapred.reduce.parallel.copies=10;
set hive.optimize.skewjoin=true;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set mapred.output.compression.type=BLOCK;
set hive.exec.dynamic.partition.mode=nostrick;
set hive.exec.dynamic.partition=true;

ALTER TABLE qxapp_dwd_hh_tmp_hive_clean_log DROP IF EXISTS PARTITION (dt='${DT_del}');
insert overwrite table qxapp_dwd_hh_tmp_hive_clean_log partition (dt='${DT}',hour)

    select
      t,
      p2,
      c1,
      c2,
      s1,
      s2,
      s3,
      e,
      se,
      r,
      a.u,
      pu,
      os,
      v,
      ua,
      tm,
      ec,
      rt,
      re,
      fv,
      br,
      ra,
      ut,
      ip,
      antispam,
      td,
      ri,
      a,
      fa,
      nu,
      device_type,
      cp,
      pr,
      st,
      dm,
      ct,
      isp_id,
      nation_id,
      region_id,
      province_id,
      city_id,
      referal,
      custom_cols,
      pf,
      p,
      p1,
      hour
    from
    (
        select
            *,
            custom_cols['vfrm'] as mkey
        from 
            longyuan.hive_tbl_base_longyuanv4_opt_play
        where 
          dt='${DT}' 
          -- and p2=1016 and c1=101601 
          and p1 in (233,232,'2_22_233','2_22_232') and t in (2, 3)
    ) a
    left join
    (   
        SELECT mkey, u
        FROM antispam_dwd.qxapp_dwd_dd_inc_hive_blacklist
        WHERE dt='${DT}'
    ) b
    on 
        a.mkey = b.mkey and a.u = b.u
    where 
        b.mkey IS NULL and b.u IS NULL
