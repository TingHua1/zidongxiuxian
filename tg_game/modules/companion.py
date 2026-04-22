from tg_game.models import FeatureModule


MODULE = FeatureModule(
    key="companion",
    name="侍妾",
    summary="管理侍妾寻找、互动、安置与访客相关玩法。",
    status="active",
    capabilities=["侍妾互动入口", "红尘寻缘快照", "洞府联动记录"],
    next_steps=["补侍妾状态字段", "补星宫侍妾联动", "区分洞府与随身状态"],
)
