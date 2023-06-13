package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private RedissonClient redissonClient;

    //private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct//当前类初始化完毕后就执行下述方法
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    /**
     * 获取消息队列中的信息
     */
    private class VoucherOrderHandler implements Runnable{
        String queueName = "stream.orders";
        @Override
        public void run() {
            while(true){
                try {
                    //1.获取消息队列中的订单信息XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS streams.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //2.判断消息获取是否成功
                    if(list == null || list.isEmpty()) {
                        //2.1 获取失败，说明没有消息，继续下一次循环
                        continue;
                    }
                    //解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    //2.2 如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    //4.ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                    //出异常了，消息会进pendingList，再处理
                    handlePendingList();
                }
            }
        }

        private void handlePendingList(){
            while(true){
                try {
                    //1.获取消息队列中的订单信息XREADGROUP GROUP g1 c1 COUNT 1 STREAMS streams.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //2.判断消息获取是否成功
                    if(list == null || list.isEmpty()) {
                        //2.1 获取失败，说明pendingList没有异常消息，结束循环
                        break;
                    }
                    //解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    //2.2 如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    //4.ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理pendingList订单异常",e);
                    //出异常了，消息会进pendingList，再处理
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
    }

    /**
     * 获取阻塞队列中信息
     * @param voucherOrder
     */
//    private class VoucherOrderHandler implements Runnable{
//        @Override
//        public void run() {
//            while(true){
//                //1.获取队列中的订单信息
//                try {
//                    VoucherOrder voucherOrder = orderTasks.take();//阻塞队列没元素会阻塞，不用担心死循环
//                    //创建订单
//                    handleVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.error("处理订单异常",e);
//                }
//            }
//        }
//    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //1.获取用户
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取锁
        boolean isLock = lock.tryLock();
        //判断是否获取锁成功
        if(!isLock){
            //获取锁失败，返回错误或重试，这里某个人获取锁失败了，说明已经抢成功了一次，直接返回错误
            log.error("不允许重复下单！");
            return;
        }
        try{
            proxy.createVoucherOrder(voucherOrder);
        }finally {
            //释放锁
            lock.unlock();
        }
    }

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static{
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }
    /**
     * 实现优惠券秒杀功能
     * @param voucherId
     * @return
     */
    private IVoucherOrderService proxy;

    /**
     * 用redis中Stream,的消费者组做消息队列
     * @param voucherId
     * @return
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //获取订单id
        long orderId = redisIdWorker.nextId("order");
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );
        //2.判断结果是否为0
        int r = result.intValue();
        if(r != 0){
            //2.1不为0，代表没有购买资格
            return Result.fail(r == 1?"库存不足！":"不能重复下单！");
        }
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //3.返回订单id
        return Result.ok(orderId);
    }

    /**
     * 阻塞队列
     * @param voucherOrder
     */
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //获取用户
//        Long userId = UserHolder.getUser().getId();
//        //1.执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(),
//                userId.toString()
//        );
//        //2.判断结果是否为0
//        int r = result.intValue();
//        if(r != 0){
//            //2.1不为0，代表没有购买资格
//            return Result.fail(r == 1?"库存不足！":"不能重复下单！");
//        }
//        //2.2为0，有购买资格，把下单信息保存到阻塞队列
//        // 保存阻塞队列
//        //2.3.创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        //2.4 订单id
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        //2.5 用户id
//        voucherOrder.setUserId(userId);
//        //2.6 代金券id
//        voucherOrder.setVoucherId(voucherId);
//        //2.7 放入阻塞队列
//        orderTasks.add(voucherOrder);
//        //3.获取代理对象
//        //获取代理对象事务
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        //3.返回订单id
//        return Result.ok(orderId);
//    }
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //1.查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        //2.判断秒杀是否开始
//        if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
//            //尚未开始
//            return Result.fail("秒杀尚未开始！");
//        }
//        //3.判断秒杀是否已经结束
//        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
//            //已经结束
//            return Result.fail("秒杀已经结束！");
//        }
//        //4.判断库存是否充足
//        if(voucher.getStock() < 1){
//            //库存不足
//            return Result.fail("库存不足！");
//        }
//        Long userId = UserHolder.getUser().getId();
//        //创建锁对象
//        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        //获取锁
//        boolean isLock = lock.tryLock();
//        //判断是否获取锁成功
//        if(!isLock){
//            //获取锁失败，返回错误或重试，这里某个人获取锁失败了，说明已经抢成功了一次，直接返回错误
//            return Result.fail("不允许重复下单！");
//        }
//        try{
//            //要拿到事务方法的代理对象，不然的话相当于用this。method()，事务会失效
//            //获取代理对象（事务相关）
//            //获取代理对象（事务相关）
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }finally {
//            //释放锁
//            lock.unlock();
//        }
//    }
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //4.1 一人一单
        Long userId = voucherOrder.getUserId();
        //synchronized (userId.toString().intern()) {//写在这的话，事务还没提交，锁就释放了，仍有线程安全问题
        //4.1.1查询订单,并发
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        //4.1.2判断订单是否存在
        if (count > 0) {
            log.error("该用户已经购买过了！");
            return;
        }
        //5.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)
                .update();
        if (!success) {
            //扣减失败
            log.error("库存不足！");
            return;
        }
        save(voucherOrder);
    }
//    @Transactional
//    public Result createVoucherOrder(Long voucherId) {
//        //4.1 一人一单
//        Long userId = UserHolder.getUser().getId();
//        //synchronized (userId.toString().intern()) {//写在这的话，事务还没提交，锁就释放了，仍有线程安全问题
//            //4.1.1查询订单,并发
//            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
//            //4.1.2判断订单是否存在
//            if (count > 0) {
//                return Result.fail("该用户已经购买过了！");
//            }
//            //5.扣减库存
//            boolean success = seckillVoucherService.update()
//                    .setSql("stock = stock - 1")
//                    .eq("voucher_id", voucherId).gt("stock", 0)
//                    .update();
//            if (!success) {
//                //扣减失败
//                return Result.fail("库存不足！");
//            }
//
//            //6.创建订单
//            VoucherOrder voucherOrder = new VoucherOrder();
//            //6.1 订单id
//            long orderId = redisIdWorker.nextId("order");
//            voucherOrder.setId(orderId);
//            //6.2 用户id
//            voucherOrder.setUserId(userId);
//            //6.3 代金券id
//            voucherOrder.setVoucherId(voucherId);
//            save(voucherOrder);
//            //7.返回订单id
//            return Result.ok(voucherId);
//        //}
//    }
}
