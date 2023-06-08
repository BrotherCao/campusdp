package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import io.lettuce.core.RedisClient;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
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
    /**
     * 实现优惠券秒杀功能
     * @param voucherId
     * @return
     */
    @Override

    public Result seckillVoucher(Long voucherId) {
        //1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //2.判断秒杀是否开始
        if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
            //尚未开始
            return Result.fail("秒杀尚未开始！");
        }
        //3.判断秒杀是否已经结束
        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
            //已经结束
            return Result.fail("秒杀已经结束！");
        }
        //4.判断库存是否充足
        if(voucher.getStock() < 1){
            //库存不足
            return Result.fail("库存不足！");
        }
        Long userId = UserHolder.getUser().getId();
        //创建锁对象
        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取锁
        boolean isLock = lock.tryLock();
        //判断是否获取锁成功
        if(!isLock){
            //获取锁失败，返回错误或重试，这里某个人获取锁失败了，说明已经抢成功了一次，直接返回错误
            return Result.fail("不允许重复下单！");
        }
        try{
            //要拿到事务方法的代理对象，不然的话相当于用this。method()，事务会失效
            //获取代理对象（事务相关）
            //获取代理对象（事务相关）
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        }finally {
            //释放锁
            lock.unlock();
        }
    }

    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        //4.1 一人一单
        Long userId = UserHolder.getUser().getId();
        //synchronized (userId.toString().intern()) {//写在这的话，事务还没提交，锁就释放了，仍有线程安全问题
            //4.1.1查询订单,并发
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            //4.1.2判断订单是否存在
            if (count > 0) {
                return Result.fail("该用户已经购买过了！");
            }
            //5.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherId).gt("stock", 0)
                    .update();
            if (!success) {
                //扣减失败
                return Result.fail("库存不足！");
            }

            //6.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            //6.1 订单id
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            //6.2 用户id
            voucherOrder.setUserId(userId);
            //6.3 代金券id
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);
            //7.返回订单id
            return Result.ok(voucherId);
        //}
    }
}
