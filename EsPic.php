<?php
namespace frontend\models;

use Yii;
use yii\elasticsearch\ActiveRecord;
use yii\httpclient\Client;

class EsPic extends ActiveRecord
{
    private $_id;
    private $_score;
    private $_version;
    public $limit = 30;

    public function attributes(){
        return ['id','title', 'cdn_path','width','height','size','cid','like_count','created_at','weight','tag','link'];
    }

    /**
     * @return 索引
     */
    public static function index(){
        return 'aiji66_pic';
    }

    /**
     * @return 类型
     */
    public static function type(){

        return 'pic';
    }

    /**
     * @return array 返回elasticsearch 映射关系
     */
    public static function mapping()
    {
        return [
            static::type() => [
                'properties' => [
                    'title'                 => ['type' => 'string',"analyzer" => "ik_max_word","search_analyzer" => "ik_max_word"],
                    'tag'                   => ['type' => 'string',"analyzer" => "ik_max_word","search_analyzer" => "ik_max_word"],
                    'id'                    => ['type' => 'long'],
                    'cdn_path'              => ['type' => 'string'],
                    'width'                 => ['type' => 'long'],
                    'height'                => ['type' => 'long'],
                    'size'                  => ['type' => 'long'],
                    'link'                  => ['type' => 'string'],
                    'cid'                   => ['type' => 'long'],
                    'like_count'            => ['type' => 'long'],
                    'comment_count'         => ['type' => 'long'],
                    'view_count'            => ['type' => 'long'],
                    'uid'                   => ['type' => 'long'],
                    'is_top'                => ['type' => 'long'],
                    'weight_updated_time'   => ['type' => 'long'],
                    'status'                => ['type' => 'long'],
                    'source'                => ['type' => 'string'],
                    'created_at'            => ['type' => 'string'],
                    'updated_at'            => ['type' => 'string'],
                ]
            ],
        ];
    }

    /**
     * @return array 返回elasticsearch 映射关系
     */
    public static function updateMapping()
    {
        $db = static::getDb();
        $command = $db->createCommand();
        $command->setMapping(static::index(), static::type(), static::mapping());
    }

    /**
     * 创建索引
     */
    public static function createIndex()
    {
        $db = static::getDb();
        $command = $db->createCommand();
        $command->createIndex(static::index(), [
            'settings' => [ /* ... */ ],
            'mappings' => static::mapping(),
            //'warmers' => [ /* ... */ ],
            //'aliases' => [ /* ... */ ],
            //'creation_date' => '...'
        ]);
    }

    /**
     * 删除索引
     */
    public static function deleteIndex()
    {
        $db = static::getDb();
        $command = $db->createCommand();
        $command->deleteIndex(static::index(), static::type());
    }

    /**
     * 查询
     */
    public function search($q,$cate,$order,$type,$plateType,$page=1,$limit=30, $showCateNum = true)
    {
        //分类筛选
        if($cate != 0){
            $filter[] = ["term" => ["cid" => $cate]];
        }
        //类型筛选
        if($type != 0){
            if($type == 4){
                $filter[] = ["term" => ["is_han" => 1]];
                $filter[] = ["term" => ["type" => 0]];
            }else{
                $filter[] = ["term" => ["type" => $type]];
            }
        }
        //板式筛选
        if($plateType != 0){
            $filter[] = ['term' => ['plate_type' => $plateType]];
        }
        $query['function_score']['query']["bool"]["must"]["multi_match"] = [
            "query" => $q,
            "fields" => ["title","tag"],
            "type" => "cross_fields",
            "operator" => "AND"
        ];
       // var_dump($query);exit;
        //排序模型
        $esWeight = Yii::$app->redis->hgetall('es_weight');
        //最大值
        $max = Yii::$app->redis->hgetall('es_max_value');
        if(!$max){
            $queryMax['aggs']['max_view'] = ["max" => ["field" => "view_num"]];
            $queryMax['aggs']['max_collect'] = ["max" => ["field" => "collect_num"]];
            $queryMax['aggs']['max_create'] = ["max" => ["field" => "created_at"]];
            $queryMax['aggs']['max_album'] = ["max" => ["field" => "album_num"]];
            $esMax = $this->find()->addAgg("max_view","max",["field" => "view_num"])
                                ->addAgg("max_collect","max",["field" => "collect_num"])
                                ->addAgg("max_create","max",["field" => "created_at"])
                                ->addAgg("max_album","max",["field" => "album_num"])
                                ->limit(0)->createCommand()->search();
            $max = [
                1 => $esMax['aggregations']['max_view']['value'],
                3 => $esMax['aggregations']['max_collect']['value'],
                5 => $esMax['aggregations']['max_create']['value'],
                7 => $esMax['aggregations']['max_album']['value'],
            ];
            Yii::$app->redis->hmset('es_max_value','max_view',$max['max_view'],'max_collect',$max['max_collect'],'max_create',$max['max_create'],'max_album',$max['max_album']);
            Yii::$app->redis->expire('es_max_value',86400);
        }
        $query['function_score']['query']["bool"]["filter"]["bool"]["must"][] = ["term" => ["status" => 1]];
        $queryCateMust = $query['function_score']['query']["bool"]["filter"]["bool"]["must"];
        $query['function_score']['query']["bool"]["filter"]["bool"]["must"][] = isset($filter) ? $filter : (object)[];
        $score = "_score*{$esWeight[1]}+(0.1*doc[\"view_count\"].value+doc[\"view_num\"].value)/{$max[1]}*{$esWeight[3]}+(doc[\"collect_num\"].value)/{$max[3]}*{$esWeight[5]}+(doc[\"album_num\"].value)/{$max[7]}*{$esWeight[7]}+(doc[\"created_at\"].value)/{$max[5]}L*{$esWeight[9]}";
        //排序
        if($order != 0) {
            switch ($order) {
                case 1:
                    $query['function_score']['script_score'] = ["script" => ["lang"=>"painless","inline"=>$score]];
                    break;
                case 2:
                    $query['function_score']['script_score'] = $this->definedSort('view_count');
                    break;
                case 3:
                    $query['function_score']['script_score'] = $this->definedSort('collect_num');
                    break;
            }
        }
        //分页
        $offset = ($page-1)*$this->limit;
        //$esResult = $this->find()->query($query)->offset($offset)->limit($limit)->createCommand();
        //$result = $esResult->db->nodes;
       // echo json_encode($esResult->queryParts);exit;
        if($showCateNum === true) {
            //如果分类为空数据和分类group一起返回,否则分开查询
            if($cate == 0) {
                $esResult = $this->find()->addAgg('group_by_cid', 'terms', ['field' => 'cid','size' => 30])->query($query)->offset($offset)->limit($limit)->createCommand()->search();
            }else {
                $esResult = $this->find()->query($query)->offset($offset)->limit($limit)->createCommand()->search();
                //重置query条件
                $query['function_score']['query']["bool"]["filter"]["bool"]["must"] = $queryCateMust;
                $cateResult = $this->find()->addAgg('group_by_cid', 'terms', ['field' => 'cid','size' => 30])->query($query)->offset(0)->limit(0)->createCommand()->search();
                $esResult["aggregations"] = $cateResult["aggregations"];
            }

        }else {
            $esResult = $this->find()->query($query)->offset($offset)->limit($limit)->createCommand()->search();
        }
//        var_dump($esResult);exit;
        return $esResult;
    }



    /**
     * es图片推荐
     */
    public function recommend($q,$cate,$page=1)
    {
        $q = mb_substr($q,0,20);
        //分类筛选
        $filter = $cate == 0 ? (object)[] : ["term" => ["cid" => $cate]];
        $query["bool"]["must"]["multi_match"] = [
            'query'     => $q,
            "type"      => "best_fields",
            'fields'    => ['title^2','tag'],
            "tie_breaker"=>          0.3,
            "minimum_should_match" => "30%"
        ];
        $query["bool"]["filter"]["bool"]["must"][] = ["term" => ["status" => 1]];
        $query["bool"]["filter"]["bool"]["must"][] = $filter;
        //分页
        $offset = ($page-1)*$this->limit;
        $esResult = $this->find()->query($query)->offset($offset)->limit(30)->asArray()->all();
        return \yii\helpers\BaseArrayHelper::getColumn($esResult, '_source');
    }

    /**
     * 获取es图片数量
     */
    public static function count()
    {
        $picCountKey = 'pic:count';
        $picCount = Yii::$app->redis->get($picCountKey);

        if(empty($picCount)){
            $client = new Client();
            $esPic = $client->createRequest()
                ->setMethod('get')
                ->setUrl("http://".Yii::$app->params['es_host'].'/aiji66_pic/pic/_count')
                ->send();
            $picCount = $esPic->data['count'];
            Yii::$app->redis->set($picCountKey,$picCount);
            Yii::$app->redis->expire($picCountKey,3600);
        }
        //图片数量
        return $picCount;
    }

    /**
     * 随机排序
     */
    public static function randomSort()
    {
        return ['is_top' => ['order'=>'desc'],'_script' => ['script' => "Math.random()",'type' => 'number','order' => 'asc']];
    }

    /**
     * es自定义排序
     */
    public function definedSort($field)
    {
        return ["script" => ["lang"=>"painless","inline" => "doc['{$field}'].value"]];
    }

    /**
     * 用户兴趣推荐
     */
    public function getInterest($query,$page,$order=1)
    {
        //分页
        $query['constant_score']['filter']['bool']['must'][] = ["term" => ["status" => 1]];
        $offset = ($page-1)*$this->limit;
        switch ($order) {
            case 1:
                $order = $this->randomSort();
                break;
            case 2:
                $order = ['_script' => ['script' => ["lang"=>"painless","inline" => "doc['view_num'].value"],'type' => 'number','order' => 'desc']];
                break;
            case 3:
                $order = ['_script' => ['script' => ["lang"=>"painless","inline" => "doc['collect_num'].value"],'type' => 'number','order' => 'desc']];
                break;
        }
        $picList = $this->find()->query($query)->orderBy($order)->limit(30)->offset($offset)->asArray()->all();
        $headers = Yii::$app->response->headers;
        $headers->set('pid',implode(',',array_column($picList,'_id')));
        return \yii\helpers\BaseArrayHelper::getColumn($picList, '_source');
    }
}
?>