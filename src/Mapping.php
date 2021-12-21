<?php
namespace Yauphp\Data\Doctrine;

use Doctrine\ORM\EntityManager;
use Yauphp\Data\IMapping;

/**
 * 数据映射接口实现
 * @author Tomix
 *
 */
class Mapping implements IMapping
{
    /**
     * 实体管理器
     * @var EntityManager
     */
    private $m_entityManager=null;

    /**
     * 注入实体管理器实例
     * @param EntityManager $value
     */
    public function setEntityManager(EntityManager $value)
    {
        $this->m_entityManager=$value;
    }

    /**
     * 根据类名获取表名
     * @param string $entityClass
     */
    public function getTableName($entityClass)
    {
        return $this->m_entityManager->getClassMetadata($entityClass)->getTableName();
    }

    /**
     * 获取所有字段名
     * @param string $entityClass
     */
    public function getFieldNames($entityClass)
    {
        return $this->m_entityManager->getClassMetadata($entityClass)->getFieldNames();
    }

    /**
     * 获取所有列名
     * @param string $entityClass
     */
    public function getColumnNames($entityClass)
    {
        return $this->m_entityManager->getClassMetadata($entityClass)->getColumnNames(null);
    }

    /**
     * 根据列名获取字段名
     * @param string $entityClass
     * @param string $columnName
     */
    public function getFieldName($entityClass,$columnName)
    {

        return $this->m_entityManager->getClassMetadata($entityClass)->getFieldName($columnName);
    }

    /**
     * 根据字段名获取列名
     * @param string $entityClass
     * @param string $fieldName
     */
    public function getColumnName($entityClass,$fieldName)
    {
        return $this->m_entityManager->getClassMetadata($entityClass)->getColumnName($fieldName);
    }

    /**
     * 根据字段名获取字段类型
     * @param string $entityClass
     * @param string $fieldName
     */
    public function getFieldType($entityClass,$fieldName){
        return $this->m_entityManager->getClassMetadata($entityClass)->getFieldMapping($fieldName)["type"];
    }

    /**
     * 替换表达式中的字段名为真实的查询列名
     * @param string $entityClass
     * @param string $expression
     * @param string $appendAlias
     */
    public function mapSqlExpression($entityClass, $expression,$appendAlias="")
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

        //替换字段名为列名
        $fields = $this->m_entityManager->getClassMetadata($entityClass)->getFieldNames();
        foreach ($fields as $field){
            $column=$this->m_entityManager->getClassMetadata($entityClass)->getColumnName($field);
            $replace=$column;
            if($appendAlias){
                $replace=$appendAlias.".".$column;
            }
            $expression=str_replace($field, $replace, $expression);
        }

        //恢复占位符内容
        foreach ($holders as $key=>$value){
            $expression=str_replace($key, $value, $expression);
        }

        return $expression;
    }

    /**
     * 获取函数名
     * @param int|string $key
     */
    public function getFunctionName($key)
    {
        switch ($key){
            case Functions::FUNCTION_BIT_AND:
                return "BIT_AND";
            default:
                return "";
        }
    }
}

