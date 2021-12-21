<?php
namespace Yauphp\Data\Doctrine;

use Doctrine\DBAL\Connection;
use Doctrine\ORM\Configuration;
use Doctrine\ORM\EntityManager;
use Doctrine\ORM\Mapping\ClassMetadataInfo;
use Yauphp\Data\IDao;
use Yauphp\Logger\ILogger;
use Yauphp\Data\IMapping;

/**
 * 基于Doctrine框架的DAO实现
 * @author Tomix
 *
 */
class DaoImpl implements IDao
{
    /**
     * 日志记录器
     * @var ILogger
     */
    private $m_logger=null;

    /**
     * 最后一条异常信息
     * @var \Exception
     */
    private $m_lastException=null;

    /**
     * 当前事务状态
     * @var boolean
     */
    private $m_transactionStatus=false;

    /**
     * Doctrine实体管理器
     * @var EntityManager
     */
    private $m_entityManager=null;

    /**
     * 数据映射工具
     * @var IMapping
     */
    private $m_mapping=null;

    /**
     * 注入日志记录器
     * @param ILogger $value
     */
    public function setLogger(ILogger $value)
    {
        $this->m_logger=$value;
    }

    /**
     * 构造函数
     * @param Connection $connection
     * @param Configuration $configuration
     */
    public function __construct(Connection $connection,Configuration $config)
    {
        $this->m_entityManager=EntityManager::create($connection, $config);
    }

    /**
     * 获取最后一次产生的异常
     * @return \Exception|null
     */
    public function getException()
    {
        return $this->m_lastException;
    }

    /**
     * 获取当前的事务状态
     * @return bool
     */
    public function getTransactionStatus()
    {
        return $this->m_transactionStatus;
    }

    /**
     * 开始事务
     */
    public function beginTransaction()
    {
        $this->m_entityManager->beginTransaction();
        $this->m_transactionStatus=true;
    }

    /**
     * 提交事务
     */
    public function commitTransaction()
    {
        $this->m_entityManager->commit();
        $this->m_transactionStatus=false;
    }

    /**
     *回滚事务
     */
    public function rollbackTransaction()
    {
        $this->m_entityManager->rollback();
        $this->m_transactionStatus=false;
    }

    /**
     * 根据ID获取实体
     * @param string $entityClass   实体类型名
     * @param mixed $id             实体ID,复合ID以键值对数组形式传入
     * @return object
     */
    public function get($entityClass,$id)
    {
        try{
            return $this->m_entityManager->find($entityClass, $id);
        }catch (\Exception $ex){
            $this->handleException($ex);
            throw $ex;
        }
    }

    /**
     * 根据过滤条件获取唯一记录
     * @param string $entityClass
     * @param string $filter
     * @param array $params
     */
    public function getOne($entityClass, $filter = null,array $params=null){
        $list=$this->list($entityClass,$filter,$params);
        if(!empty($list)){
            return $list[0];
        }
        return null;
    }

    /**
     * 加载数据
     * @param object $entity 实体对象,引用传递
     * @param string|array $fields 查询字段,用实体属性名表示,留空表示按主键字段查询
     * @throws \Exception
     * @return boolean
     */
    public function load(&$entity, $fields = null)
    {
        try{
            $entityClass=get_class($entity);
            if(empty($fields)){
                $identifier=$this->m_entityManager->getClassMetadata($entityClass)->getIdentifier();
                $idValues=Tool::getFieldValues($entity, $identifier);
                $_entity = $this->m_entityManager->find($entityClass, $idValues);
                if($_entity){
                    $entity=$_entity;
                    return $entity;
                }
            }else{
                $criteria=Tool::getFieldValues($entity, $fields);
                $_entity=$this->m_entityManager->getRepository($entityClass)->findOneBy($criteria);
                if($_entity){
                    $entity=$_entity;
                    return $entity;
                }
            }
        }catch (\Exception $ex){
            $this->handleException($ex);
            throw $ex;
        }
        return false;
    }

    /**
     * 保存或更新记录,成功则返回true
     * @param object $entity
     * @return boolean
     */
    public function persist($entity)
    {
        try{
            $this->m_entityManager->persist($entity);
            $this->m_entityManager->flush();
            return true;
        }catch (\Exception $ex){
            $this->handleException($ex);
            throw $ex;
        }
    }

    /**
     * 批量更新数据
     * @param string $entityClass       实体类型名
     * @param array  $fieldValues       更新的字段值,键为字段名,值为字段值
     * @param string $filter            过滤表达式
     * @param array $params             输入参数
     */
    public function updates($entityClass,$fieldValues=[], $filter = null, array $params=null)
    {
        if(empty($fieldValues)){
            return 0;
        }
        if(is_null($params)){
            $params=[];
        }
        try{
            //主表别名
            $baseName=$entityClass;
            if(strrpos($entityClass, "\\")>0){
                $baseName=substr($entityClass, strrpos($entityClass, "\\")+1);
            }
            $alias="_".$baseName;

            //编译表达式
            $joins=[];
            $_filter=$this->compileExpression($entityClass, $alias, $filter,$joins);

            //查询DQL
            $dql="UPDATE ".$entityClass." ".$alias." SET ";
            $update="";
            foreach ($fieldValues as $field=>$value){
                if($update!=""){
                    $update.=",";
                }
                $valueKey=$field."_".uniqid();
                $update.=$alias.".".$field."=:".$valueKey;
                $params[$valueKey]=$value;
            }
            $dql.=$update;

            if($joins){
                foreach ($joins as $join => $_alias){
                    $dql.=" JOIN ".$alias.".".$join." ".$_alias;
                }
            }

            //filter
            if($_filter){
                $dql.=" WHERE ".$_filter;
            }

            //query
            $query=$this->m_entityManager->createQuery($dql);
            $query->setParameters($params);
            $this->m_entityManager->clear();
            return  $query->execute();
        }catch (\Exception $ex){
            $this->handleException($ex);
            throw $ex;
        }
    }

    /**
     * 删除实体
     * @param object $entity   实体对象
     */
    public function delete($entity)
    {
        try{
            $this->m_entityManager->remove($entity);
            $this->m_entityManager->flush();
            return true;
        }catch (\Exception $ex){
            $this->handleException($ex);
            throw $ex;
        }
    }

    /**
     * 根据实体ID删除数据
     * @param string $entityClass  实体类型名
     * @param mixed $id             实体ID,复合ID以键值对数组形式传入
     * @return object
     */
    public function deleteById($entityClass,$id)
    {
        try{
            $entity=$this->m_entityManager->find($entityClass, $id);
            if($entity){
                $this->m_entityManager->remove($entity);
                $this->m_entityManager->flush();
            }
            return $entity;
        }catch (\Exception $ex){
            $this->handleException($ex);
            throw $ex;
        }
    }

    /**
     * 批量删除数据
     * @param string $entityClass 实体类型名
     * @param string $filter 过滤表达式
     * @param array $params  输入参数
     */
    public function deletes($entityClass, $filter = null, array $params=null)
    {
        if(is_null($params)){
            $params=[];
        }

        try {
            //主表别名
            $baseName=$entityClass;
            if(strrpos($entityClass, "\\")>0){
                $baseName=substr($entityClass, strrpos($entityClass, "\\")+1);
            }
            $alias="_".$baseName;

            //编译表达式
            $joins=[];
            $_filter=$this->compileExpression($entityClass, $alias, $filter,$joins);

            //查询DQL
            $dql="DELETE ".$entityClass." ".$alias;
            if($joins){
                foreach ($joins as $join => $_alias){
                    $dql.=" JOIN ".$alias.".".$join." ".$_alias;
                }
            }

            //filter
            if($_filter){
                $dql.=" WHERE ".$_filter;
            }

            //query
            $query=$this->m_entityManager->createQuery($dql);
            $query->setParameters($params);
            return $query->execute();
        }catch (\Exception $ex){
            $this->handleException($ex);
            throw $ex;
        }
    }

    /**
     * 数据查询
     * @param string $entityClass       实体类型名
     * @param string $filter            过滤表达式
     * @param array $params             输入参数
     * @param string $orderBy           排序表达式
     * @param number $offset            起始记录号
     * @param number $limit             返回记录数
     * @param string $groupBy           分组表达式
     */
    public function list($entityClass, $filter = null,array $params=null, $orderBy = null, $offset = null, $limit = null,$groupBy=null)
    {
        if(is_null($params)){
            $params=[];
        }
        try{
            //主表别名
            $rootAlias="_t_0";

            //所有父级的join对象
            $toOneJoinMap=[];
            $this->getToOneJoinMap($entityClass,$toOneJoinMap,$rootAlias);

            //编译表达式
            $joins=[];
            $_filter=$this->compileExpression($entityClass, $rootAlias, $filter,$toOneJoinMap,$joins);
            $_order=$this->compileExpression($entityClass, $rootAlias, $orderBy,$toOneJoinMap,$joins);
            $_group=$this->compileExpression($entityClass, $rootAlias, $groupBy,$toOneJoinMap,$joins);

            //拼接DQL,把表达式里需要用到的join加进来
            $dql="SELECT ".$rootAlias;
            $joinExp="";
            foreach ($joins as $path){
                $info=$toOneJoinMap[$path];
                $join=$info["join"];
                $alias=$info["alias"];
                $dql.=",".$alias;
                $joinExp.=" LEFT JOIN ".$join." ".$alias;
            }
            $dql.=" FROM ".$entityClass." ".$rootAlias.$joinExp;

            //filter
            if($_filter){
                $dql.=" WHERE ".$_filter;
            }
            //group by
            if($_group){
                $dql.=" GROUP BY ".$_group;
            }
            //order by
            if($_order){
                $dql.=" ORDER BY ".$_order;
            }

            //query
            $query=$this->m_entityManager->createQuery($dql);
            $query->setParameters($params);

            //offset limit
            if(!is_null($offset) && !is_null($limit)){
                $query->setMaxResults($limit);
                $query->setFirstResult($offset);
            }

            //return
            $rs=$query->getResult();
            return $rs;
        }catch (\Exception $ex){
            $this->handleException($ex);
            throw $ex;
        }
    }

    /**
     * 查询计数
     * @param string $entityClass   实体类型名
     * @param string $filter        过滤表达式
     * @param array $params         输入参数
     */
    public function count($entityClass, $filter = null,array $params=null)
    {
        if(is_null($params)){
            $params=[];
        }
        try{

            //主表别名
            $rootAlias="_t_0";

            //所有父级的join对象
            $toOneJoinMap=[];
            $this->getToOneJoinMap($entityClass,$toOneJoinMap,$rootAlias);

            //编译表达式
            $joins=[];
            $_filter=$this->compileExpression($entityClass, $rootAlias, $filter,$toOneJoinMap,$joins);

            //count字段
            $countField=$rootAlias.".".$this->m_entityManager->getClassMetadata($entityClass)->getIdentifier()[0];

            //查询DQL
            $dql="select COUNT(".$countField.") from ".$entityClass." ".$rootAlias;
            if($joins){
                foreach ($joins as $path){
                    $info=$toOneJoinMap[$path];
                    $dql.=" LEFT JOIN ".$info["join"]." ".$info["alias"];
                }
            }

            //filter
            if($_filter){
                $dql.=" WHERE ".$_filter;
            }

            //query
            $query=$this->m_entityManager->createQuery($dql);
            $query->setParameters($params);
            $count=$query->getSingleScalarResult();
            return $count;
        }catch (\Exception $ex){
            $this->handleException($ex);
            throw $ex;
        }
    }

    /**
     * 数据聚合统计($funcMap,$groupFields至少一个不为空;函数名必须与对应的数据库一致)
     * @param string|object $entityClass 实体类名
     * @param string $filter             过滤表达式
     * @param array $params              输入参数
     * @param string $sort               排序表达式
     * @param array $funcMap             统计函数,键为函数名,值为统计字段(值为数组时,第一个元素为统计字段,第二个为返回字段)
     * @param array $groupFields         分组统计字段,可选
     */
    public function group($entityClass,$filter = null,array $params=null,$order=null,$funcMap=[],$groupFields=[])
    {
        //统计函数与分组字段参数,至少一个不分空
        if(empty($funcMap) && empty($groupFields)){
            return false;
        }
        if(is_null($params)){
            $params=[];
        }

        try{
            //主表别名
            $rootAlias="_t_0";

            //所有父级的join对象
            $toOneJoinMap=[];
            $this->getToOneJoinMap($entityClass,$toOneJoinMap,$rootAlias);

            //关联查询实体
            $joins=[];

            //dql
            $dql="";

            //统计函数
            if(!empty($funcMap)){
                foreach ($funcMap as $func=>$fd){
                    if($dql!=""){
                        $dql.=",";
                    }
                    $_fd=$fd;       //统计字段
                    $_returnFd=$fd; //返回字段
                    if(is_array($fd)){
                        $_fd=$fd[0];
                        $_returnFd=$fd[1];
                    }
                    $_fd=$this->compileExpression($entityClass, $rootAlias, $_fd,$toOneJoinMap,$joins);
                    $dql.=$func."(".$_fd.") AS ".$_returnFd;
                }
            }

            //group fields
            $groupBy="";
            if(!empty($groupFields)){
                $groupBy=$groupFields;
                if(is_array($groupFields)){
                    $groupBy=implode(",", $groupFields);
                }
                $groupBy=$this->compileExpression($entityClass, $rootAlias, $groupBy,$toOneJoinMap,$joins);
                if($dql!=""){
                    $dql.=",";
                }
                $dql.=$groupBy;
            }

            //编译表达式
            $_filter=$this->compileExpression($entityClass, $rootAlias, $filter,$toOneJoinMap,$joins);
            $_order=$this->compileExpression($entityClass, $rootAlias, $order,$toOneJoinMap,$joins);

            //DQL
            $dql="SELECT ".$dql." FROM ".$entityClass." ".$rootAlias;
            if($joins){
                foreach ($joins as $path){
                    $info=$toOneJoinMap[$path];
                    $dql.=" JOIN ".$info["join"]." ".$info["alias"];
                }
            }

            //filter
            if($_filter){
                $dql.=" WHERE ".$_filter;
            }

            //group by
            if($groupBy){
                $dql.=" GROUP BY ".$groupBy;
            }

            //order by
            if($_order){
                $dql.=" ORDER BY ".$_order;
            }

            //query
            $query=$this->m_entityManager->createQuery($dql);
            $query->setParameters($params);
            $rs=$query->getResult();
            return $rs;
        }catch (\Exception $ex){
            $this->handleException($ex);
            throw $ex;
        }
    }

    /**
     * 执行原生SQL语句
     * @param string $sql   SQL语句
     * @param array $params 输入参数
     */
    public function sqlUpdate($sql,array $params=null)
    {
        if(is_null($params)){
            $params=[];
        }
        try{
            $types=Tool::detectParamTypes($params);
            return $this->m_entityManager->getConnection()->executeUpdate($sql,$params,$types);
        }catch (\Exception $ex){
            $this->handleException($ex);
            throw $ex;
        }
    }

    /**
     * 原生SQL查询,返回二维数组
     * @param string $sql               SQL语句
     * @param array $params             输入参数
     * @param number $offset            起始记录号
     * @param number $limit             返回记录数
     */
    public function sqlQuery($sql,array $params=null, $offset = null, $limit = null)
    {
        if(is_null($params)){
            $params=[];
        }
        try{
            //分页表达式
            if(!is_null($limit) && !is_null($offset)){
                $platform=$this->m_entityManager->getConnection()->getDriver()->getDatabasePlatform()->getName();
                if(strtolower($platform)=="mysql"){
                    $sql.=" LIMIT ".$offset.",".$limit;
                }//else 其它平台待实现
            }
            $types=Tool::detectParamTypes($params);
            return $this->m_entityManager->getConnection()->executeQuery($sql,$params,$types)->fetchAll();
        }catch (\Exception $ex){
            $this->handleException($ex);
            throw $ex;
        }
    }

    /**
     * 获取映射工具
     * @return IMapping
     */
    public function getMapping():IMapping
    {
        if($this->m_mapping==null){
            $this->m_mapping=new Mapping();
            $this->m_mapping->setEntityManager($this->m_entityManager);
        }
        return $this->m_mapping;
    }


    /**
     * 获取N对一的映射关系表达式
     * @param string $entityClass    类型名称
     * @param array $joinMap         映射关系:键为路径标识,值[join]为关联表达式,值[alias]为关联对象别名
     * @param string $rootAlias      根别名
     * @param string $parentPath     上级路径
     */
    private function getToOneJoinMap($entityClass,&$joinMap,$rootAlias="",$parentPath="")
    {
        $metaData=$this->m_entityManager->getClassMetadata($entityClass);
        $mappings = $metaData->getAssociationMappings();
        foreach ($mappings as $field => $mapping){
            $type=$mapping["type"];
            if(($type&ClassMetadataInfo::ONE_TO_ONE)>0 || ($type&ClassMetadataInfo::MANY_TO_ONE)>0){
                $_path=$parentPath;
                if($_path!=""){
                    $_path.=".";
                }
                $_path.=$field;
                //$joinMap[$_path]="_".$field."_".count($joinMap);
                $alias="_".$field."_".(count($joinMap)+1);
                $join=$rootAlias.".".$field;
                if($parentPath!=""){
                    $join=$joinMap[$parentPath]["alias"].".".$field;
                }
                $joinMap[$_path]=["join"=>$join,"alias"=>$alias];
                $targetEntity=$mapping["targetEntity"];
                $this->getToOneJoinMap($targetEntity, $joinMap,$alias,$_path);
            }
        }
    }

    /**
     * 编译通用查询表达成为DQL表达
     * @param string $entityClass 实体名
     * @param string $rootAlias   实体别名
     * @param string $expression  表达式
     * @param array  $allJoinMap  所有joins
     * @param array $joinPaths    表达式用到的joins路径
     * @return string             返回编译后的表达式式
     */
    private function compileExpression($entityClass,$rootAlias, $expression,$allJoinMap=[],&$joinPaths=[])
    {
        if(empty($expression)){
            return $expression;
        }

        //保护参数占位符
        $pattern="/:([\w]{1,})/";
        $matches=[];
        preg_match_all($pattern, $expression,$matches);
        $holders=[];
        foreach ($matches[1] as $paramName){
            $key=uniqid();
            $holders[$key]=$paramName;
            $expression=str_replace(":".$paramName, ":".$key, $expression);
        }
        //attribute.type.model.id=1
        //替换带前缀的表达式
        $pattern="/([\w\.]{1,})\.([\w]{1,})/";
        $matches=[];
        preg_match_all($pattern, $expression,$matches);
        if(count($matches[0])>0){
            for($i=0;$i<count($matches[0]);$i++){
                if(is_numeric($matches[0][$i])){
                    continue;
                }
                //根据路径取出关联信息
                $path=$matches[1][$i];
                $field=$matches[2][$i];
                if(!array_key_exists($path, $allJoinMap)){
                    //throw new \Exception("Not found: ".$path);
                    continue;
                }
                $info=$allJoinMap[$path];
                $alias=$info["alias"];

                //复制所有路径到输出参数
                $paths=explode(".", $path);
                $_path="";
                foreach ($paths as $p){
                    if($_path!=""){
                        $_path.=".";
                    }
                    $_path.=$p;
                    if(!in_array($_path, $joinPaths)){
                        $joinPaths[]=$_path;
                    }
                }

                //替换表达式
                $key=uniqid();
                $holders[$key]=$alias.".".$field;
                $search=$matches[0][$i];
                $expression=str_replace($search, $key, $expression);
            }
        }

        //替换主表表达式
        $fields = $this->m_entityManager->getClassMetadata($entityClass)->getFieldNames();
        $expression=" ".$expression." ";
        foreach ($fields as $field){
            //$expression=str_replace($field, $rootAlias.".".$field, $expression);
            $pattern="/([^\w]{1,1})(".$field.")([^\w]{1,1})/";
            $expression=preg_replace($pattern, "\${1}".$rootAlias.".".$field."\${3}", $expression);
        }
        $expression=trim($expression);

        //恢复占位符内容
        foreach ($holders as $key=>$value){
            $expression=str_replace($key, $value, $expression);
        }
        return $expression;
    }

    /**
     * 异常处理
     * @param \Exception $ex
     */
    private function handleException(\Exception $ex)
    {
        $this->m_lastException=$ex;
        if($this->m_logger){
            $this->m_logger->logException($ex,"dao-ex");
        }
    }
}

