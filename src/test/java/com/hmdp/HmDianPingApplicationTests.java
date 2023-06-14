package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

@SpringBootTest
public class HmDianPingApplicationTests {

    @Resource
    private ShopServiceImpl shopService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private CacheClient cacheClient;

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    private ExecutorService es = Executors.newFixedThreadPool(500);

    @Test
    public void testSaveShop() throws InterruptedException {
        //shopService.saveShopToRedis(1L,10L);
        Shop shop = shopService.getById(1L);
        cacheClient.setWithLogicalExpire(CACHE_SHOP_KEY + 1L,shop,10L, TimeUnit.SECONDS);
    }

    @Test
    void testId() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(300);
        Runnable task = () -> {
            for(int i = 0;i < 100;i++){
                long id = redisIdWorker.nextId("order");
                System.out.println("id = " + id);
            }
            latch.countDown();
        };
        long begin = System.currentTimeMillis();
        for(int i = 0;i< 300;i++){
            es.submit(task);
        }
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("time = " + (end - begin));
    }

    @Test
    void loadShopData(){
        //1.查询店铺信息
        List<Shop> list = shopService.list();
        //2.把店铺分组，按照typeId分组，id一致的放到一个集合
        Map<Long,List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        //3.分批完成写入redis
        for(Map.Entry<Long,List<Shop>> entry : map.entrySet()){
            //3.1 获取类型id
            Long typeId = entry.getKey();
            String key = "shop:geo:" + typeId;
            //3.2 获取同类型的店铺的集合
            List<Shop> value = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>();
            //3.3 写入redis GEOADD key 经度 维度 member
            for (Shop shop : value){
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),
                        new Point(shop.getX(),shop.getY())
                ));

            }
            stringRedisTemplate.opsForGeo().add(key,locations);
        }

    }
}
