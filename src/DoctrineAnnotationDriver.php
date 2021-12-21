<?php
namespace Yauphp\Data\Doctrine;

use Doctrine\ORM\Mapping\Driver\AnnotationDriver;
use Doctrine\Common\Annotations\AnnotationRegistry;
use Doctrine\Common\Annotations\SimpleAnnotationReader;
use Doctrine\Common\Annotations\CachedReader;
use Doctrine\Common\Cache\ArrayCache;

/**
 * ORM注解驱动
 * @author Tomix
 *
 */
class DoctrineAnnotationDriver extends AnnotationDriver
{
    public function __construct(array $paths=null){
        AnnotationRegistry::registerUniqueLoader('class_exists');
        $reader = new SimpleAnnotationReader();
        $reader->addNamespace('Doctrine\ORM\Mapping');
        $cachedReader = new CachedReader($reader, new ArrayCache());
        parent::__construct($cachedReader, $paths);
    }
}

