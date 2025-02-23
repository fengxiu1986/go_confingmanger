# go_confingmanger
go实现了动态配置(etcd->file->env) <br />
将etcd 或者 file 或者 env变量 保存到go的map本地，从而实现本地配置。<br />
etcd/file/env 的变量新增 修改  删除都会同步到go 本地map，从而实现同步维护

