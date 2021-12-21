<?php
namespace Yauphp\Data\Doctrine;

use Doctrine\DBAL\Connection;

/**
 * DAO工具类
 * @author Tomix
 *
 */
class Tool
{
    /**
     * 取出实体的字段值
     * @param object $entity            实体对象
     * @param string|array $fields      字段集:逗号分隔的字段名或字段名数组
     * @return array                    键值对数组:键为字段名,值为字段值
     */
    public static function getFieldValues($entity,$fields)
    {
        if(!is_array($fields)){
            $fields=explode(",", $fields);
        }
        $values=[];
        foreach ($fields as $field){
            $values[$field]=$entity->$field;
        }
        return $values;
    }

    /**
     * 检测参数类型
     * @param array $params
     */
    public static function detectParamTypes($params=null)
    {
        $types=[];
        if(is_array($params)){
            foreach ($params as $key => $value){
                if(is_array($value)){
                    $types[$key]=Connection::PARAM_STR_ARRAY;
                }
            }
        }
        return $types;
    }
}

