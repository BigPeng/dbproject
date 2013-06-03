每个人在自己的文件夹下修改，修改后提交到github
在每个文件夹下面都建了一个txt文件，这个文件没有什么用，只是git不跟踪空目录，
我特意添加了这个文件


示例：我在pengji目录下创建了一个btree.c
git add pengji/btree.c
git commit -m "Add btree in pengji by pengjiqun"
git remote add origin https://github.com/BigPeng/dbproject.git
输入用户名和密码（需要在github上注册）