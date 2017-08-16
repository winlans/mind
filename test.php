<?php
/**
 * Created by Tony Tao
 * Date: 2016/4/29
 * Time: 15:36
 */

namespace BI\Service\ElasticSearch;

use BI\Application;
use BI\Entity\Friends;
use BI\Entity\Mission;
use BI\Service\BiFile;
use BI\Service\ElasticSearch\Query\Condition\Boolean;
use BI\Service\ElasticSearch\Query\Condition\Exists;
use BI\Service\ElasticSearch\Query\Condition\Fuzzy;
use BI\Service\ElasticSearch\Query\Condition\Ids;
use BI\Service\ElasticSearch\Query\Condition\Match;
use BI\Service\ElasticSearch\Query\Condition\Range;
use BI\Service\ElasticSearch\Query\Condition\Regexp;
use BI\Service\ElasticSearch\Query\Condition\Term;
use BI\Service\ElasticSearch\Query\Condition\Terms;
use BI\Service\ElasticSearch\Query\Condition\WildCard;
use BI\Service\ElasticSearch\Query\Query;
use BI\Service\ElasticSearch\Search\Search;
use BI\Service\ElasticSearch\Search\Sort\Sort;
use BI\Service\Security;

use Chat\Operator\BaseChatOperator;
use Chat\Operator\BaseFetchMessage;

use Mission\Model\MissionConst;
use Exception;

class SearchHelper
{
    const SIZE = 11;

    const WILDCARD_1 = '*,';
    const WILDCARD_2 = ',*';
    const REGEXP_MSG = '&lt;@USER|[a-z0-9]{1,32}*';
    const REGEXP_NAME = '<@USER|[a-z0-9]{1,32}>';

    /** @var BiFile  */
    private $biFile;

    /** @var  ESManager */
    private $esManager;

    private $uuid;
    
    /**
     * Search query params 
     */
    private $queryParam;

    public function __construct(BiFile $biFile, ESManager $esManager)
    {
        $this->biFile = $biFile;
        $this->esManager = $esManager;
        $this->uuid = Application::$_singleton['session']->get( Security::KEY_SESSION_USER )['uuid'];
    }
    
    /**
     * Get sniffer result of query
     * @return NULL|string
     */
    public function getSnifferResult() {
        return $this->queryParam;
    }

    /**
     * @param string $keywords
     * @param int $companyID the company the user belongs to
     * @param int $page
     * @param int $size
     * @return array
     */
    public function contactSearch($keywords, $companyID = null,  $page = 1, $size = 100){
        $search = new Search();
        $search->setIndex('contact');
        $search->setSource(array('id',  'suid', 'uuid', 'cid', 'status', 'profile_id', 'user_profile_path', 'work_name',
           'logo_path', 'logo_id', 'name', 'address', 'company_name'));

        $companyID = $companyID ? array('company_' . $companyID) : array();

        $idsCompany = new Ids($companyID, 'company');
        $matchCompanyName = new Match('name', $keywords);
        $fuzzyCompanyName = new Fuzzy('name', $keywords, 3, 2, 100);
        $shouldCompanyName = new Boolean(Boolean::SHOULD, array($matchCompanyName, $fuzzyCompanyName));
        $mustCompany = new Boolean(Boolean::MUST, array($idsCompany, $shouldCompanyName));
        $mustCompany->setBoost(1000);
        $_shouldCompanyName = clone $shouldCompanyName;
        $_shouldCompanyName->setBoost(100);

        $matchWorkName = new Match('work_name', $keywords);
        $fuzzyWorkName = new Fuzzy('work_name', $keywords, 3, 2, 100);
        $shouldWorkName = new Boolean(Boolean::SHOULD, array($matchWorkName, $fuzzyWorkName));
        $mustUser = new Boolean(Boolean::MUST, array($shouldWorkName));
        $mustUser->setBoost(10);
        $_shouldWorkName = clone $shouldWorkName;
        $_shouldWorkName->setBoost(1);

        $should = new Boolean(Boolean::SHOULD, array($mustCompany, $_shouldCompanyName, $mustUser, $_shouldWorkName));
        $query = new Query($should);
        $search->setQuery($query);
        $page = $page == 0 ? $page : $page -1;
        $search->setFrom($page * $size);
        $search->setSize($size);
        $searchResponse = $this->esManager->search($search);
        
        $debugHttpRequest = $this->esManager->getDebugHttpRequest();
        $this->queryParam = $debugHttpRequest->getRawData();
        
        if($searchResponse->isError()){
            return array();
        }

        $groups = Friends::getGroups();
        foreach($groups as $k => $v)
            $groups[$k] = array();

        return $searchResponse->getSearchHits()->getHits();
    }

    /**
     * 搜索公司admin
     * 1. 根据关键词搜索全局用户
     * 2. 根据关键词+公司id搜索公司中有职位者
     * 3. 可分页
     * @param string $keywords
     * @param int $cid
     * @param int $page
     * @return array
     */
    public function adminSearch( $keywords = '', $cid = 0, $page = 1 )
    {
        $search = new Search();
        $search->setIndex('contact');
        $search->setSource(array('id', 'psid', 'suid', 'uuid', 'cid', 'profile_id', 'user_profile_path', 'work_name',
            'p_name', 'logo_path', 'logo_id', 'name', 'address', 'company_name'));

        $matchWorkName = new Match('work_name', $keywords);
        $fuzzyWorkName = new Fuzzy('work_name', $keywords, 3, 2, 100);
        $shouldWorkName = new Boolean(Boolean::SHOULD, array($matchWorkName, $fuzzyWorkName));
        $mustUser = new Boolean(Boolean::MUST, array($shouldWorkName));
        $mustUser->setBoost(10);
        $_shouldWorkName = clone $shouldWorkName;
        $_shouldWorkName->setBoost(1);

        if ($cid) {
            $cidMust = new Term('cid', $cid);
            $psidMust = new Exists('psid');
            $shouldUser = new Boolean(Boolean::SHOULD, array($mustUser, $_shouldWorkName));
            $should = new Boolean(Boolean::MUST, array($cidMust, $psidMust,  $shouldUser));
        } else {
            $should = new Boolean(Boolean::SHOULD, array($mustUser, $_shouldWorkName));
        }

        $query = new Query($should);
        $search->setQuery($query);
        $page = $page == 0 ? $page : $page -1;
        $search->setFrom($page * self::SIZE);
        $search->setSize(self::SIZE);

        $searchResult = array();
        $searchResponse = $this->esManager->search($search);
        if($searchResponse->isError()){
            return $searchResult;
        }
        $responseData = $searchResponse->getSearchHits()->getHits();

        foreach($responseData as $key => $value){
            if ($value['_source']['uuid'] != $this->uuid) {
                $searchResult[] = $value['_source'];
            }
        }

        return $searchResult;
    }

    /**
     * @param string $keywords
     * @param $cid
     * @return array company => array(
     * company => array(
     * array(companyArray1, companyArray2,...)
     * )
     */
    public function companySearch($keywords, $cid)
    {
        $search = new Search();
        $search->setIndex('contact');
        $search->setSource(array('cid', 'logo_id', 'logo_path', 'name', 'address'));

        $matchCompanyName = new Match('name', $keywords);
        $fuzzyCompanyName = new Fuzzy('name', $keywords, 3, 2, 100);
        $shouldCompanyName = new Boolean(Boolean::SHOULD, array($matchCompanyName, $fuzzyCompanyName));
        $mustCompany = new Boolean(Boolean::MUST, array($shouldCompanyName));
        $mustCompany->setBoost(1000);
        $_shouldCompanyName = clone $shouldCompanyName;
        $_shouldCompanyName->setBoost(100);

        $should = new Boolean(Boolean::SHOULD, array($mustCompany, $_shouldCompanyName));
        $query = new Query($should);
        $search->setQuery($query);

        $searchResult = array(
            'company' => array(),
        );
        $searchResponse = $this->esManager->search($search);
        if ($searchResponse->isError()) {
            return $searchResult;
        }
        $responseData = $searchResponse->getSearchHits()->getHits();

        foreach ($responseData as $key => $data) {
            if ($data['_type'] == 'company') {

                if ($data['_source']['logo_id']) {
                    $logo = $this->biFile->getFileLink($data['_source']['logo_path'],
                        BiFile::FILE_TYPE_COMPANY_LOGO, array('size' => BiFile\CompanyProfileProcessor::PROFILE_SIZE_3));
                    $data['_source']['logo_path'] = $logo;
                } else {
                    $logo = $this->biFile->getFileLink($data['_source']['logo_path'],
                        BiFile::FILE_TYPE_DEFAULT_PROFILE, array('size' => BiFile\DefaultImageProcessor::PROFILE_SIZE_3));
                    $data['_source']['logo_path'] = $logo;
                }

                if ($data['_source']['cid'] == $cid) {
                    continue;
                }

                $searchResult['company'][] = $data['_source'];
            }
        }
        return $searchResult;
    }

    /**
     * $firstTime 第一个时间筛选的条件
     * $secondTime 第二个时间筛选的条件 两者是或
     *
     * @param $psid
     * @param int $page
     * @param $status
     * @param $type
     * @param $isSelf
     * @return array
     */
    public function missionCalendarList($psid, $page = 1, $status, $type, $isSelf)
    {
//        var_dump(1497341029 > $start);exit;
//        $mission_status = new Terms('status', array(Mission::MISSION_STATUS_DOING));
//
//        $mustStatus = new Boolean(Boolean::MUST, array($mission_status));
//        $query = new Query($mustStatus);
//        $this->search->setQuery($query);
//        $this->search->setFrom($page * self::SIZE);
//        $this->search->setSize(self::SIZE);
//        $responseData = $this->searchResult($this->search);
//var_dump($responseData);exit;
//        date_default_timezone_set('UTC');
        $search = new Search();
        $search->setIndex('mission');
        $search->setSource(
            array(
                'id', 'name', 'description', 'publisher_psid', 'publisher_name', 'psids', 'level', 'status', 'type',
                'created', 'created_timestamp', 'start', 'start_timestamp', 'end', 'actual_start_timestamp', 'actual_end', 'actual_end_timestamp'
            )
        );

        $missionStatus = $missionStatusReset = array();
        switch ($status) {
            case Mission::MISSION_STATUS_TODO:
                $missionStatus = array(Mission::MISSION_STATUS_TODO, Mission::MISSION_STATUS_PENDING);
                $missionStatusReset = new Term('status', Mission::MISSION_STATUS_RESET);
                break;
            case Mission::MISSION_STATUS_DOING:
                $missionStatus = array(Mission::MISSION_STATUS_DOING);
                break;
            case Mission::MISSION_STATUS_DONE:
                $missionStatus = array(Mission::MISSION_STATUS_DONE);
                break;
            case Mission::MISSION_STATUS_STORAGE:
                $missionStatus = array(Mission::MISSION_STATUS_STORAGE);
                break;
            case Mission::MISSION_STATUS_PAUSE:
                $missionStatus = array(Mission::MISSION_STATUS_PAUSE);
                break;
        }

        $mustType = array();

        if ($type != MissionConst::TYPE_ALL) {
            $type = new Term('type', $type);
            $mustType = new Boolean(Boolean::MUST, array($type));
        }

        $first = new wildcard('psids', $psid);
        $second = new wildcard('psids', $psid . self::WILDCARD_2);
        $three = new wildcard('psids', self::WILDCARD_1 . $psid);
        $four = new wildcard('psids', self::WILDCARD_1 . $psid . self::WILDCARD_2);
        $wildcard = new Boolean(Boolean::SHOULD, array($first, $second, $three, $four));
        $mustPsids = new Boolean(Boolean::MUST, array($wildcard));

        $publisher_psid = new Term('publisher_psid', $psid);
        $mustPublisherPsid = new Boolean(Boolean::MUST, array($publisher_psid));

        switch ($isSelf) {
            case MissionConst::IS_SELF_ALL:
                $should = new Boolean(Boolean::SHOULD, array($mustPsids, $mustPublisherPsid));
                break;
            case MissionConst::IS_SELF_YOURS:
                $should = $mustPublisherPsid;
                break;
            case MissionConst::IS_SELF_OTHERS:
                $should = $mustPsids;
                break;
            default:
                $should = new Boolean(Boolean::SHOULD, array($mustPsids, $mustPublisherPsid));
        }

        $mission_status = new Terms('status', $missionStatus);
        $mustStatus = new Boolean(Boolean::MUST, array($mission_status));

        if ($missionStatusReset) {
            $mustStatus = new Boolean(Boolean::SHOULD, array($mission_status, $missionStatusReset));
        }

        $must = new Boolean(Boolean::MUST, array($mustStatus, $should));
        if ($mustType) {
            $must = new Boolean(Boolean::MUST, array($mustStatus, $should, $mustType));
        }

        $query = new Query($must);
        $search->setQuery($query);

        $created = array(
            'created' => array(
                'order' => Sort::ORDER_DESC
            )
        );
        $sort = new Sort($created);
        $search->setSort($sort);

        $page = $page == 0 ? $page : $page -1;
        $search->setFrom($page * self::SIZE);
        $search->setSize(self::SIZE);

        $searchResult = array();
        $searchResponse = $this->esManager->search($search);
        if($searchResponse->isError()){
            return $searchResult;
        }
        $responseData = $searchResponse->getSearchHits()->getHits();

        foreach ($responseData as $key => $data) {
            $searchResult[] = $data['_source'];
        }
        return $searchResult;
    }

    /**
     * 根据psid 当前用户是否能查看这些mission, 并且在传入的mids里面
     * @param array $mids
     * @return array
     */
    public function fetchNameDescByMids($mids)
    {
        $search = new Search();
        $search->setIndex('mission');
        $search->setSource(
            array(
                'id', 'name', 'description', 'publisher_psid', 'publisher_name', 'psids', 'level', 'status', 'type',
                'created', 'start', 'start_timestamp', 'end', 'actual_start_timestamp', 'actual_end', 'actual_end_timestamp'
            )
        );
        $id = new Terms('id', $mids);
        $idMust = new Boolean(Boolean::MUST, array($id));

        $mission_status = new Term('status', Mission::MISSION_STATUS_DELETE);
        $mustStatus = new Boolean(Boolean::MUST_NOT, array($mission_status));

        $must =  new Boolean(Boolean::MUST, array($idMust, $mustStatus));

        $query = new Query($must);
        $search->setQuery($query);

        $searchResult = array();
        $searchResponse = $this->esManager->search($search);
        if($searchResponse->isError()){
            return $searchResult;
        }
        $responseData = $searchResponse->getSearchHits()->getHits();

        foreach ($responseData as $key => $data) {
            $searchResult[] = $data['_source'];
        }

        return $searchResult;
    }

    /**
     * @param $keywords
     * @param array $status
     * @param $psid
     * @param $page
     * @param $sort
     * @return array
     */
    public function missionSearch($keywords, $status, $psid, $page = 0, $sort)
    {
        $search = new Search();
        $search->setIndex('mission');
        $search->setSource(
            array(
                'id', 'name', 'description', 'publisher_psid', 'publisher_name', 'psids', 'level', 'status', 'type',
                'created', 'start', 'start_timestamp', 'end', 'actual_start_timestamp', 'actual_end', 'actual_end_timestamp'
            )
        );
        //  match mission name
        $matchMissionName = new Match('name', $keywords);
        $fuzzyMissionName = new Fuzzy('name', $keywords, 3, 2, 100);
        $shouldMissionName = new Boolean(Boolean::SHOULD, array($matchMissionName, $fuzzyMissionName));
        $mustMissionName = new Boolean(Boolean::MUST, array($shouldMissionName));
        $mustMissionName->setBoost(100);
        $_mustMissionName = clone $mustMissionName;
        $_mustMissionName->setBoost(1);

        //  match description
        $matchMissionDescription = new Match('description', $keywords);
        $fuzzyMissionDescription = new Fuzzy('description', $keywords, 3, 2, 100);
        $shouldMissionDescription = new Boolean(Boolean::SHOULD, array($matchMissionDescription, $fuzzyMissionDescription));
        $mustMissionDescription = new Boolean(Boolean::MUST, array($shouldMissionDescription));
        $mustMissionDescription->setBoost(100);
        $_mustMissionDescription = clone $mustMissionDescription;
        $_mustMissionDescription->setBoost(1);

        //  match publisher_name
        $matchWorkName = new Match('publisher_name', $keywords);
        $fuzzyWorkName = new Fuzzy('publisher_name', $keywords, 3, 2, 100);
        $shouldWorkName = new Boolean(Boolean::SHOULD, array($matchWorkName, $fuzzyWorkName));
        $mustUser = new Boolean(Boolean::MUST, array($shouldWorkName));
        $mustUser->setBoost(10);
        $_shouldWorkName = clone $shouldWorkName;
        $_shouldWorkName->setBoost(1);

        $mission_status = new Terms('status', $status);

        $publisher_psid = new Term('publisher_psid', $psid);

        $first = new wildcard('psids', $psid);
        $second = new wildcard('psids', $psid . self::WILDCARD_2);
        $three = new wildcard('psids', self::WILDCARD_1 . $psid);
        $four = new wildcard('psids', self::WILDCARD_1 . $psid . self::WILDCARD_2);
        $mustPsids = new Boolean(Boolean::SHOULD, array($first, $second, $three, $four, $publisher_psid));

        $should = new Boolean(
            Boolean::SHOULD,
            array(
                $mustMissionName,
                $_mustMissionName,
                $mustMissionDescription,
                $_mustMissionDescription,
                $mustUser,
                $_shouldWorkName
            )
        );

        $mustStatus = new Boolean(Boolean::MUST, array($should, $mission_status, $mustPsids));
        $query = new Query($mustStatus);
        $search->setQuery($query);

        switch ($sort) {
            case 0:
                $created = array(
                    'created' => array(
                        'order' => Sort::ORDER_DESC
                    )
                );
                break;
            case 1:
                $created = array(
                    'actual_end_timestamp' => array(
                        'order' => Sort::ORDER_DESC
                    )
                );
                break;
            default:
                $created = array(
                    'created' => array(
                        'order' => Sort::ORDER_DESC
                    )
                );
                break;
        }
        $search->setSort(new Sort($created));

        $page = $page == 0 ? $page : $page -1;
        $search->setFrom($page * self::SIZE);
        $search->setSize(self::SIZE);

        $searchResult = array();
        $searchResponse = $this->esManager->search($search);
        if($searchResponse->isError()){
            return $searchResult;
        }
        $responseData = $searchResponse->getSearchHits()->getHits();

        foreach ($responseData as $key => $data) {
            if ($data['_source']['status'] == Mission::MISSION_STATUS_DELETE) {
                unset($responseData[$key]);
            } else {
                $searchResult[] = $data['_source'];
            }
        }

        return $searchResult;
    }

    /**
     * 会议室搜索
     * @param $keywords
     * @param int $page
     * @return array
     */
    public function conferenceSearch($keywords, $page = 1)
    {
        $search = new Search();
        $search->setIndex('reservation');
        $search->setSource(
            array('id', 'cid', 'crid', 'psid', 'title', 'attendance', 'projector', 'video', 'start', 'start_timestamp',
                'end', 'end_timestamp', 'created', 'status', 'room_name', 'work_name', 'user_profile_path'
            )
        );
        //  match mission name
        $matchTitle = new Match('title', $keywords);
        $fuzzyTitle = new Fuzzy('title', $keywords, 3, 2, 100);
        $shouldTitle = new Boolean(Boolean::SHOULD, array($matchTitle, $fuzzyTitle));
        $mustTitle = new Boolean(Boolean::MUST, array($shouldTitle));
        $mustTitle->setBoost(10);
        $_mustTitle = clone $mustTitle;
        $_mustTitle->setBoost(1);

        //  match publisher_name
        $matchWorkName = new Match('work_name', $keywords);
        $fuzzyWorkName = new Fuzzy('work_name', $keywords, 3, 2, 100);
        $shouldWorkName = new Boolean(Boolean::SHOULD, array($matchWorkName, $fuzzyWorkName));
        $mustUser = new Boolean(Boolean::MUST, array($shouldWorkName));
        $mustUser->setBoost(10);
        $_shouldWorkName = clone $shouldWorkName;
        $_shouldWorkName->setBoost(1);

        $should = new Boolean(
            Boolean::SHOULD,
            array(
                $mustTitle,
                $_mustTitle,
                $mustUser,
                $_shouldWorkName
            )
        );

        $mustStatus = new Boolean(Boolean::MUST, array($should));
        $query = new Query($mustStatus);
        $search->setQuery($query);
        $page = $page == 0 ? $page : $page -1;
        $search->setFrom($page * self::SIZE);
        $search->setSize(self::SIZE);

        $searchResult = array();
        $searchResponse = $this->esManager->search($search);
        if($searchResponse->isError()){
            return $searchResult;
        }
        $responseData = $searchResponse->getSearchHits()->getHits();

        foreach ($responseData as $key => $data) {
            $searchResult[] = $data['_source'];
        }
        return $searchResult;
    }

    /**
     * $uuid, $suid, $channelGid, $channelSuid 这三个参数，
     * 如果要搜索所有频道，传入$uuid, $suid 以及该用户的所有群的群号 即$channelGid 为数组
     * 如果要搜索某个个人聊天的频道，传入$uuid 或者$suid  外加 $channelSuid
     * 如果要搜索某个群频道，传入$channelGid
     *
     * @param string $keywords 要搜索的词
     * @param string $uuid 当前用户uuid 如果是搜索某个群 则无需提供
     * @param string $psid 当前用户suid 如果是搜索某个群 则无需提供
     * @param int|int[] $channelGid 要搜索的群号 如果提供了群号 则搜索该群内的聊天记录
     * @param string $channelUid 聊天对象的id 搜索本人与此人之间的单对单聊天记录
     * @param int $form
     * @param int $type 消息类型 | 1文本信息 2图片信息 3文件信息 4 post信息
     * @param $start
     * @param $end
     * @param int $page 页数 从第0页开始
     * @param int $size 每页条数
     * @return array 数组内的每一项结构如下，其中_id字段为mongodb message的 _id：
     *      array (size=5)
     * '_index' => string 'chat_record' (length=11)
     * '_type' => string 'form1' (length=5)
     * '_id' => string '5805e93b77044f195c549a8b' (length=24)
     * '_score' => float 1
     * '_source' =>
     * array (size=11)
     * 'msg' => string '我看到 123等3条消息' (length=26)
     * 'owner' => string '08697e13fbb300a2fa0b2af8ad3a4d43' (length=32)
     * 'owner_name' => string 'albert' (length=6)
     * 'gid' => string '' (length=0)
     * 'owner_profile' => string 'upload-register-photo-img6.jpg' (length=30)
     * 'uid2' => string '2469cc5dd81f92d4959b09807c73fcf5' (length=32)
     * 'uid1' => string '08697e13fbb300a2fa0b2af8ad3a4d43' (length=32)
     * 'msg_type' => int 1
     * 'time' => float 1476782395177
     * 'status' => int 1
     * @throws Exception
     */
    public function searchChatRecord($keywords, $uuid = null, $psid = null, $channelGid = null, $channelUid = null, $form = 0,
                                     $type = 0, $start, $end, $page = 1, $size = 200)
    {
        $search = new Search();

        $search->setIndex('chat_record');
        $search->setSource(array(
            '_id',
            'msg',
            'name',
            'msg_type',
            'owner',
            'owner_name',
            'owner_profile',
            'owner_profile_id',
            'time',
            'detail',
            'gid',
            'uid1',
            'uid2',
            'uid1_name',
            'uid2_name',
            'uid1_profile',
            'uid2_profile',
            'uid1_profile_id',
            'uid2_profile_id',
            'status'
        ));
//        $must = new Boolean(Boolean::MUST);
//
//        $regexp_msg = new Regexp('msg', self::REGEXP_MSG);
//        $regexp_name = new Regexp('name', self::REGEXP_MSG);
//        $must_not = new Boolean(Boolean::MUST_NOT, array($regexp_msg, $regexp_name));
//
////        $must = new Boolean(Boolean::MUST, array($must_not_msg, $must_not_name));
//        $must->addChild($must_not);
//        $query = new Query($must);
//        $search->setQuery($query);
//        $search->setFrom($page * $size);
//        $search->setSize($size);
//
//        $searchResponse = $this->esManager->search($search);var_dump($searchResponse);
//        if($searchResponse->isError()){
//            return array();
//        }
//        var_dump($searchResponse->getSearchHits()->getHits());exit;

        //match keyword
        $useSearch['query'] = [];

        $useSearch['query']['bool']['should'] = [
            'multi_match' => [
                "query" => $keywords,
                "type" => "best_fields",
                "fields" => [ "msg", "name" ],
                "tie_breaker" => 0.3,
                "minimum_should_match" => "30%"
            ],
            'fuzzy' => [
                "like_text" => $keywords,
                "fields" => [ "msg", "name" ],
                "prefix_length" => 2,
                "max_expansions" => 100
            ]
        ];

        $useSearch['query']['bool']['must_not'][] = [
            'regexp' => ['msg' => self::REGEXP_MSG]
        ];

        if ($type !== 0 && in_array($type, BaseFetchMessage::getMsgTypes())) {
            $useSearch['query']['bool']['must'][] =[
                'term' => ['msg_type' => $type]
            ];
        }

        if ($form !== 0 && $form == BaseChatOperator::YOUR_CHANNEL) {
            $useSearch['query']['bool']['must'][] =[
                'terms' => ['owner' => [$uuid,$psid]]
            ];
        }

        if ($form !== 0 && $form == BaseChatOperator::MEMBER_CHANNEL) {
            $useSearch['query']['bool']['must_not'][] =[
                'terms' => ['owner' => [$uuid,$psid]]
            ];
        }

        if ($start && $end) {
            $useSearch['query']['bool']['must'][] = [
                'range' =>[
                    'time' => [
                        'gt' => $start,
                        'lt' => $end
                    ]
                ]
            ];
        }

        $error = 0;
        //search in a group channel
        if ($channelGid && !is_array($channelGid)) {
            $useSearch['query']['bool']['must'][] = [
                'term' => ['gid' => $channelGid]
            ];
        }
        else if ($uuid || $psid) {
            $shouldIds = new Boolean(Boolean::SHOULD);

            //search in a personal channel
            if($channelUid){
                $uid = $uuid ?: $psid;
                $useSearch['query']['bool']['should'][] = [
                    'term' => ['uid1' => $uid],
                    'term' => ['uid2' => $uid],
                    'term' => ['uid1' => $channelGid],
                    'term' => ['uid2' => $channelGid],
                ];
            }

            //search all channels
            else if (is_array($channelGid)) {
                $useSearch['query']['bool']['should'][] = [
                    'term' => ['uid1' => $psid],
                    'term' => ['uid2' => $psid],
                    'term' => ['uid1' => $uuid],
                    'term' => ['uid2' => $uuid],
                    'terms' => ['gid' => $channelGid]
                ];
            } else
                $error = 1;
        } else
            $error = 1;

        if ($error) return array();
//            throw new Exception('Parameters passed to the function are incorrect.');

        $query = new Query($must);
        $search->setQuery($query);
        $page = $page == 0 ? $page : $page -1;
        $search->setFrom($page * $size);
        $search->setSize($size);

        $searchResponse = $this->esManager->search($search);

        // add debuger
        $debugHttpRequest = $this->esManager->getDebugHttpRequest();
        $this->queryParam = $debugHttpRequest->getRawData();

        if($searchResponse->isError()){
            return array();
        }
        return $searchResponse->getSearchHits()->getHits();
    }
}
