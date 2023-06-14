package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Resource
    private IShopService shopService;
    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //1.
        //Shop shop = queryWithPassThrough(id);
        //2.自己封装的工具类
        //Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY,id,Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);
        //互斥锁解决缓存击穿
        //Shop shop = queryWithMutex(id);
        //逻辑过期解决缓存击穿
        //1.
        //Shop shop = queryWithLogicalExpire(id);
        //2.
        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY,id,Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);
        if(shop == null){
            return Result.fail("店铺不存在");
        }
        //7.返回
        return Result.ok(shop);
    }

    //TODO 不建议这样创建线程池，一定要自己手动创建线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public Shop queryWithLogicalExpire(Long id){
        String key = CACHE_SHOP_KEY + id;
        //1.从redis中查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if(StrUtil.isBlank(shopJson)){
            //3.未命中。直接返回null
            return null;
        }
        //4.命中，需要i先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())) {
            //5.1 未过期，直接返回商铺信息
            return shop;
        }
        //5.2 已过期，需要缓存重建
        //6.缓存重建
        String lockKey = LOCK_SHOP_KEY + id;
        //6.1获取互斥锁
        boolean isLock = tryLock(lockKey);
        //6.2判断互斥锁获取是否成功
        if(isLock) {
            //TODO 获取锁成功后DoubleCheck，查询redis中是否有缓存，如果有的话无需重建缓存
            //TODO 6.3成功，开启独立线程实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //重建
                    this.saveShopToRedis(id, 20L);
                }catch(Exception e){
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    unLock(lockKey);
                }
            });

        }
        //6.4返回过期的商铺信息
        return shop;
    }
    /**
     * 基于互斥锁解决缓存击穿问题
     * @param id
     * @return
     */
    public Shop queryWithMutex(Long id){
        //1.从redis查询商铺缓存
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if(StrUtil.isNotBlank(shopJson)){
            //3.存在，直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //判断命中的是否是空值
        //到这了，shopJson只能是null或者空值“”
        if(shopJson != null){
            //返回一个错误信息
            return null;
        }
        //实现缓存重建
        //4.1 获取互斥锁
        Shop shop = null;
        String lockKey = LOCK_SHOP_KEY + id;
        try {
            boolean isLock = tryLock(lockKey);
            //4.2 判断是否成功
            //4.3 失败，休眠并重试，
            if (!isLock) {
                Thread.sleep(50);
                return queryWithMutex(id);
            }
//            String shopTemp = stringRedisTemplate.opsForValue().get(key + id);
//            //双重校验，虽然获取到锁了，但是可能是别的线程重建缓存完了
//            if(StrUtil.isNotBlank(shopTemp)){
//                //3.存在，直接返回
//                return JSONUtil.toBean(shopTemp, Shop.class);
//            }
            //4.4 成功，根据id查询
            shop = getById(id);
            //模拟重建的延时
            //Thread.sleep(200);
            //不存在，返回错误
            if (shop == null) {
                //将空值写入redis
                stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_NULL_TTL, TimeUnit.MINUTES);
                //返回
                return null;
            }
            //6.存在，写入redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        }catch (InterruptedException e){
            throw new RuntimeException(e);
        }finally {
            //7.释放互斥锁
            unLock(lockKey);
        }
        //8.返回
        return shop;
    }

    //穿透
    public Shop queryWithPassThrough(Long id){
        //1.从redis查询商铺缓存
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if(StrUtil.isNotBlank(shopJson)){
            //3.存在，直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //判断命中的是否是空值
        //到这了，shopJson只能是null或者空值“”
        if(shopJson != null){
            //返回一个错误信息
            return null;
        }
        //4.不存在，根据id查询数据库
        Shop shop = getById(id);

        //5.不存在，返回错误
        if(shop == null){
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        //6.存在，写入redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //7.返回
        return shop;
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

    /**
     * 给热点数据预热
     * @param id
     * @param expireSeconds 逻辑过期时间
     */
    public void saveShopToRedis(Long id,Long expireSeconds){
        //1.查询店铺数据
        Shop shop = getById(id);
        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3.写入Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(redisData));
    }
    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            return Result.fail("店铺id不能为空");
        }
        //1.更新数据库
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }

    @Override
    public Result queryShopByTypeId(Integer typeId, Integer current, Double x, Double y) {
        //1.判断是否需要根据坐标查询
        if(x == null || y == null){
            //不需要坐标查询，按数据库查询
            Page<Shop> page = shopService.query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            //返回数据
            return Result.ok(page.getRecords());
        }
        //2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        //3.查询redis,按照距离排序、分页、结果：shopId\distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        //4.解析出id
        if(results == null){
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if(list.size() <= from){
            return Result.ok(Collections.emptyList());
        }
        //4.1截取从from到end
        List<Long> ids = new ArrayList<>(list.size());
        Map<String,Distance> distanceMap = new HashMap<>();
        list.stream().skip(from).forEach(result -> {
            //获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            //获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr,distance);
        });
        //5.根据id查询shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        //6.返回
        return Result.ok(shops);
    }
}
