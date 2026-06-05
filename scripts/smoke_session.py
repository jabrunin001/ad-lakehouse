import sys
sys.path.insert(0, "/opt/app")
from streaming.spark_session import build_spark
s = build_spark("smoke")
s.sql("CREATE NAMESPACE IF NOT EXISTS lh.bronze")
print("NAMESPACES:", [r.namespace for r in s.sql("SHOW NAMESPACES IN lh").collect()])
s.stop()
