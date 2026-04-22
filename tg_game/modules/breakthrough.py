from tg_game.models import FeatureModule


MODULE = FeatureModule(
    key="breakthrough",
    name="突破",
    summary="记录结丹、元婴、化神等大境界突破动作与结果快照。",
    status="active",
    capabilities=["突破命令入口", "突破结果快照", "手动高风险执行"],
    next_steps=["补充真实突破文案解析", "记录材料缺失与失败原因", "加入突破前检查清单"],
)
