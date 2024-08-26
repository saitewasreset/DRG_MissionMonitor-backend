# API v0.8.0

API 返回类型：

```typescript
interface Response<T> {
  code: number;
  message: string;
  data: T;
}
```

对于每一个 API，下文将只给出`T`的类型。

## 概览（`./api/general`）

### 任务概览(`./`)

`T = GeneralInfo`

```typescript
// “之前”与“最近”的分界线为所有有效局数 * 0.1（若所得的值小于10，则取10）.
interface DeltaData {
  prev: number; // 考虑“之前”任务的数值
  recent: number; // 考虑“最近”任务的数值
  total: number; // 考虑所有任务的数值
}

interface GeneralInfo {
  gameCount: number;
  validRate: number; // 游戏有效率
  totalMissionTime: number;
  averageMissionTime: DeltaData;
  uniquePlayerCount: number; // 遇到不同玩家的数量（按Steam名称区分）
  openRoomRate: DeltaData; // 公开房间比例（若有至少一个非好友玩家，则判断为公开房间）
  passRate: DeltaData; // 任务通过率
  averageDifficulty: DeltaData;
  averageKillNum: DeltaData;
  averageDamage: DeltaData;
  averageDeathNumPerPlayer: DeltaData;
  averageMineralsMined: DeltaData;
  averageSupplyCountPerPlayer: DeltaData;
  averageRewardCredit: DeltaData;
}
```

### 任务类型信息（`./mission_type`）

`T = MissionInfo`

```typescript
interface MissionTypeInfo {
  averageDifficulty: number;
  averageMissionTime: number;
  averageRewardCredit: number;
  creditPerMinute: number; // 总奖励代币数 / 总任务时间（分钟）
  missionCount: number;
  passRate: number;
}

interface MissionInfo {
  missionTypeMap: Record<string, string>; // mission_game_id -> 任务中文名称
  missionTypeData: Record<string, MissionTypeInfo>; // mission_game_id -> MissionTypeInfo
}
```

### 玩家信息（`./player`）

`T = PlayerData`

```typescript
interface PlayerInfo {
  averageDeathNum: number;
  averageMineralsMined: number;
  averageReviveNum: number;
  averageSupplyCount: number;
  averageSupplyEfficiency: number; // 每份补给最多回复50%弹药（不含特长），故定义为2 * 弹药比例变化量；若大于1（“补给大师”特长），仍保留
  characterInfo: Record<string, number>; // 该玩家选择每个角色的次数：character_game_id -> 选择次数
  validMissionCount: number; // 该玩家有效**游戏局数**
}

interface PlayerData {
  characterMap: Record<string, string>; // character_game_id -> 角色中文名
  playerData: Record<string, PlayerInfo>; // player_name -> PlayerInfo
}
```

### 角色选择次数（`./character_info`）

`T = CharacterInfo`

```typescript
interface CharacterInfo {
  characterCount: Record<string, number>; // character_game_id -> 选择次数
  characterMapping: Record<string, string>; // character_game_id -> 角色中文名
}
```

### 角色信息（`./character`）

`T = CharacterGeneralInfo`

```typescript
interface CharacterGeneralData {
  validCount: number; // 有效**数据数**（采用玩家指数）
  reviveNum: number;
  deathNum: number;
  mineralsMined: number;
  supplyCount: number;
  supplyEfficiency: number;
}

interface CharacterGeneralInfo {
  characterInfo: Record<string, CharacterGeneralData>; // character_game_id -> CharacterGeneralData
  characterMapping: Record<string, string>; // character_game_id -> 角色中文名
}
```

## 伤害（`./api/damage`）

### 玩家伤害信息（`./`）

`T = OverallDamageInfo`

```typescript
interface PlayerDamageInfo {
  damage: Record<string, number>; // entity_game_id -> 总计受到该玩家伤害
  kill: Record<string, number>; // entity_game_id -> 该玩家总计击杀数
  ff: {
    cause: Record<string, { gameCount: number; damage: number }>; // 承受玩家 -> { gameCount: number; damage: number }
    take: Record<string, { gameCount: number; damage: number }>; // 造成玩家 -> { gameCount: number; damage: number }
  };
  averageSupplyCount: number;
  validGameCount: number; // 有效**游戏局数**
}

interface OverallDamageInfo {
  info: Record<string, PlayerDamageInfo>; // player_name -> PlayerDamageInfo
  entityMapping: Record<string, string>; // entity_game_id -> 中文名
}
```

### 武器伤害信息（`./weapon`）

`T = Record<string, WeaponDamageInfo>` weapon_game_id -> WeaponDamageInfo

```typescript
interface WeaponDamageInfo {
  damage: number; // 总计伤害
  friendlyFire: number; // 总计友伤
  heroGameId: string; // 拥有该武器的角色的game_id
  mappedName: string; // 该武器的中文名
  validGameCount: number; // 有效**游戏局数**
}
```

### 角色伤害信息（`./character`）

`T = Record<string, CharacterDamageInfo>` character_game_id -> CharacterDamageInfo

```typescript
interface CharacterDamageInfo {
  damage: number; // 总计造成伤害
  friendlyFire: {
    cause: number; // 总计造成友伤
    take: number; // 总计受到友伤
  };
  validGameCount: number; // 有效**数据数**
  mappedName: string; // 该角色中文名
}
```

### 敌人信息（`./entity`）

`T = EntityData`

```typescript
interface EntityData {
  damage: Record<string, number>; // entity_game_id -> damage
  kill: Record<string, number>; // entity_game_id -> kill_num
  entityMapping: Record<string, string>; // entity_game_id -> 中文名
}
```

## 任务（`./api/mission`）

### 任务列表（`./mission_list`）

```typescript
type T = {
  missionInfo: MissionInfo[];
  missionTypeMapping: Record<string, string>; // mission_type_id -> 任务类型中文名
};
```

```typescript
interface MissionInfo {
  missionId: number;
  beginTimestamp: number; // 任务开始时间戳
  missionTime: number; // 任务进行时间
  missionTypeId: string;
  hazardId: number;
  missionResult: number; // 0 -> 已完成； 1 -> 失败； 2 -> 放弃
  rewardCredit: number; // 奖励代币数量
  missionInvalid: boolean;
  missionInvalidReason: string; // 若任务有效，则为""
}
```

### 任务信息（`./<int:mission_id>/info`）

`T = MissionGeneralInfo`

```typescript
interface MissionGeneralInfo {
  missionId: number;
  missionBeginTimestamp: number;
  missionInvalid: boolean;
  missionInvalidReason: string;
}
```

### 本任务玩家角色信息（`./<int:mission_id>/basic`）

`T = Record<string, string>` player_name -> character_game_id

### 任务概览（`./<int:mission_id>/general`）

`T = MissionGeneralData`

```typescript
interface MissionGeneralPlayerInfo {
  heroGameId: string;
  playerRank: number; // 玩家“蓝等”
  characterRank: number; // 所选角色“红等”
  characterPromotion: number; // 所选角色晋升次数
  presentTime: number; // 该玩家本任务中的游戏时间
  reviveNum: number; // 救人次数
  deathNum: number; // 倒地次数
  playerEscaped: number; // 是否在任务结束时成功撤离
}

interface MissionGeneralData {
  beginTimeStamp: number;
  hazardId: number;
  missionResult: number;
  missionTime: number;
  missionTypeId: string;
  playerInfo: Record<string, MissionGeneralPlayerInfo>; // player_name -> MissionGeneralPlayerInfo
  rewardCredit: number;
  totalDamage: number;
  totalKill: number;
  totalMinerals: number; // 总计矿石采集量
  totalNitra: number; // 总计硝石采集量
  totalSupplyCount: number;
}
```

### 任务玩家伤害统计（`./<int:mission_id>/damage`）

```typescript
type T = {
  info: Record<string, PlayerDamageInfo>; // player_name -> PlayerDamageInfo
  entityMapping: Record<string, string>; // entity_game_id -> 中文名
};
```

```typescript
interface FriendlyFireInfo {
  cause: Record<string, number>;
  take: Record<string, number>;
}

interface PlayerDamageInfo {
  damage: Record<string, number>; // entity_game_id -> 总计伤害
  kill: Record<string, number>; // entity_game_id -> 总计击杀数
  ff: FriendlyFireInfo;
  supplyCount: number; // 补给份数
}
```

### 任务武器伤害统计（`./<int:mission_id>/weapon`）

`T = Record<string, WeaponDamageInfo>` weapon_id -> WeaponDamageInfo

```typescript
interface WeaponDamageInfo {
  damage: number; // 本任务中总计造成伤害
  friendlyFire: number; // 本任务中总计造成友伤
  heroGameId: string; // 拥有该武器的角色的character_game_id
  mappedName: string; // 武器中文名
}
```

### 任务资源采集统计（`./<int:mission_id>/resource`）

```typescript
type T = {
  info: Record<string, PlayerResourceInfo>; // player_name -> PlayerResourceInfo
  resourceMapping: Record<string, string>; // resource_game_id -> 资源中文名
};
```

```typescript
interface PlayerResourceInfo {
  resource: Record<string, number>; // resource_game_id -> 本局中该玩家采集量
  supply: {
    // 玩家补给信息，每个元素为一次补给
    ammo: number; // 回复弹药量比例
    health: number; // 回复生命值比例
  }[];
}
```

### 任务玩家原始 KPI（`./<int:mission_id>/kpi`）

`T = MissionKPIInfo[]` 每个元素为一个角色子类型的 KPI 信息

```typescript
interface KPIComponent {
  name: string; // 该KPI组成部分的中文名
  value: number; // 该KPI组成部分的数值
  weight: number; // 该KPI组成部分的权重
  sourceThis: number; // 该KPI组成部分的该玩家的原始值
  sourceTotal: number; // 该KPI组成部分的该所有玩家的原始值
}

interface MissionKPIInfo {
  playerName: string;
  heroGameId: string;
  subtypeId: number; // 玩家所选角色的子类型（例如，辅助型侦察与输出型侦察）
  subtypeName: string; // 子类型对应的中文名
  weightedKill: number; // 加权击杀数
  weightedDamage: number; // 加权伤害
  priorityDamage: number; // 高威胁目标伤害
  reviveNum: number; // 救人次数
  deathNum: number; // 倒地次数
  friendlyFire: number; // 造成友伤
  nitra: number; // 硝石采集量
  supplyCount: number; // 补给份数
  resourceTotal: number; // 总计资源采集量
  component: KPIComponent[]; // 每个元素为一个KPI组成部分的详细信息
  rawKPI: number; // 原始KPI
}
```

## KPI（`./api/kpi`）

### 当前 KPI 配置信息（`./`）

```typescript
type T = {
  character: Record<string, Record<string, SubTypeKPIInfo>>; // character_game_id -> subtype_id -> SubTypeKPIInfo
  version: string; // 当前KPI版本
};
```

```typescript
interface SubTypeKPIInfo {
  subtypeName: string; // 该角色子类型的中文名
  priorityTable: Record<string, number>; // 高威胁目标权值表：entity_game_id -> 权值
  weightList: number[]; // 该子类型KPIComponent[]中每个元素对应的权值
}
```

### 角色权值表（`./weight_table`）

`T = WeightTableData[]` 每个元素为一种 entity 的权值信息

```typescript
interface WeightTableData {
  entityGameId: string;
  priority: number; // 高威胁目标权值
  driller: number; // 钻机权值
  gunner: number; // 枪手权值
  engineer: number; // 工程权值
  scoutA: number; // 辅助型侦察权值
  scoutB: number; // 输出型侦察权值
}
```

### 角色修正因子$\psi$（`./raw_data_by_promotion`）

```typescript
type T = Record<
  number, // 角色id
  Record<
    number, // 晋升区间
    {
      data: number[]; // 不同玩家该角色在该晋升区间下的原始KPI列表
      factor: number; // 计算出的修正因子
      median: number; // 原始KPI中位数
      average: number; // 原始KPI平均数
      std: number; // 原始KPI标准差
    }
  >
>;
```

### 人数及角色分配修正因子$\Gamma$（`./gamma`）

`type T = Record<string, GammaInnerInfo>` "kill", "damage", "nitra", "minerals" -> GammaInnerInfo

```typescript
type GammaInnerInfo = Record<
  string, // character_game_id
  {
    gameCount: number; // 有效**数据数**
    value: number; // 数据总值
    avg: number; // 数据平均值
    ratio: number; // 修正因子
  }
>;
```

### 玩家 KPI 信息（`./player_kpi`）

```typescript
type T = Record<
  string, // player_name
  {
    count: number; // 总计玩家指数
    KPI: number; // 玩家KPI
    byCharacter: Record<string, PlayerCharacterKPIInfo>; // character_game_id -> PlayerCharacterKPIInfo
  }
>;
```

```typescript
interface MissionKPIInfo {
  missionId: number;
  beginTimestamp: number;
  presentTime: number;
  playerIndex: number; // 该玩家在该任务中的玩家指数
  characterFactor: number; // 角色修正因子
  rawKPI: number; // 原始KPI
}

interface PlayerCharacterKPIInfo {
  count: number; // 该玩家在该角色上的总计玩家指数
  KPI: number; // 该玩家在该角色上的KPI
  characterGameId: string;
  characterSubtype: string;
  missionKPIList: MissionKPIInfo[]; // 每个元素为该角色其中一次任务的KPI信息
}
```
