# alibaba-dts-go
使用 go 语言，反序列化阿里巴巴提供的 dts 输出的 kafka 消息

## 1. 使用 gogen-avro ，以 avsc 文件自动生成 go 文件

```$xslt
gogen-avro ./avro record.avsc
``` 


## 2. 在代码中引入生成的 avro 包，示例代码如 main.go 所示

本项目参考了 https://github.com/LioRoger/subscribe_example 的代码
