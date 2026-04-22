from tg_game.models import FeatureModule


MODULE = FeatureModule(
    key="market",
    name="市集",
    summary="关注商品价格、上架下架、扫货规则和成交记录。",
    status="active",
    capabilities=[
        "价格监控列表",
        "上架规则模板",
        "成交流水记录",
        "扫货与补货提醒",
    ],
    next_steps=[
        "整理商品价格来源",
        "定义买卖决策规则",
        "补市集操作执行器",
    ],
)
