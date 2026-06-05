# transform/run.py
import sys

from streaming.spark_session import build_spark
from transform import dim_campaign, fact_event

BUILDERS = {
    "dim_campaign": dim_campaign.build,
    "fact_event": fact_event.build,
}
GROUPS = {"silver": ["dim_campaign", "fact_event"]}


def main() -> None:
    target = sys.argv[1] if len(sys.argv) > 1 else "silver"
    names = GROUPS.get(target, [target])
    spark = build_spark(f"transform-{target}")
    try:
        for name in names:
            print(f"[transform] building {name}")
            BUILDERS[name](spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
