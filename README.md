Atlas 使用 Janusgraph作为存储，而Janusgraph实际底层存储可以是（es + berkeleyje）。
Atlas 的里面保存的元数据可以通过AtlasClientV2进行上报和修改。
本示例主要实现：
1、上报数据给Atlas
2、Janusgraph quickstart
