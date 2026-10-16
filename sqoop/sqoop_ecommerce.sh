source /etc/profile
source ~/.bashrc

sqoop import \
--connect jdbc:postgresql://external_postgres_db/external \
--username external \
--password external \
--table raw_ecommerce \
--target-dir /user/root/stating_zone/raw_ecommerce \
--delete-target-dir \
--as-parquetfile \
-m 1