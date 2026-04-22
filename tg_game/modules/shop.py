from tg_game.models import FeatureModule


MODULE = FeatureModule(
    key="shop",
    name="商城",
    summary="记录氪金、充值和商城入口相关消息。",
    status="active",
    capabilities=["商城入口命令", "商城快照", "帮助文档入口"],
    next_steps=["补商城商品快照", "联动玩法文档中的氪金说明", "补安全提醒"],
)
