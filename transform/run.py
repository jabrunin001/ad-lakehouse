# transform/run.py
import sys

from streaming.spark_session import build_spark
from transform import dim_campaign, fact_event, gold_delivery, gold_fill, gold_pacing

BUILDERS = {
    "dim_campaign": dim_campaign.build,
    "fact_event": fact_event.build,
    "gold_delivery": gold_delivery.build,
    "gold_fill": gold_fill.build,
    "gold_pacing": gold_pacing.build,
}
GROUPS = {
    "silver": ["dim_campaign", "fact_event"],
    "gold": ["gold_delivery", "gold_fill", "gold_pacing"],
    "all": ["dim_campaign", "fact_event", "gold_delivery", "gold_fill", "gold_pacing"],
}


def main() -> None:
    target = sys.argv[1] if len(sys.argv) > 1 else "all"
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
