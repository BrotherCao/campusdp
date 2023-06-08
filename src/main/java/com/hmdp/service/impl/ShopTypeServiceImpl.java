package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author brocao
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryTypeList() {
        //1.从redis中查询商铺类型缓存
        String key = "cache:shopType:";
        Long size = stringRedisTemplate.opsForList().size(key);
        //2.有则直接返回
        if(size != 0){
            List<String> list = stringRedisTemplate.opsForList().range(key, 0, size - 1);
            List<ShopType> typeList = new ArrayList<>();
            for(String str : list){
                typeList.add(JSONUtil.toBean(str,ShopType.class));
            }
            return Result.ok(typeList);
        }
        //3.没有则查询db
        List<ShopType> typeList = query().orderByAsc("sort").list();
        //4.不存在，返回错误
        if(typeList.isEmpty()){
            return Result.fail("查询错误");
        }
        //5.存在，写入redis
        for(ShopType shopType : typeList){
            stringRedisTemplate.opsForList().rightPush(key,JSONUtil.toJsonStr(shopType));
        }
        //6.返回
        return Result.ok(typeList);
    }
}
