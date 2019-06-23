package com.hdp.project.kafka.producer;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Liuyongzhi
 * @description: 定义Company对象
 * @date 2019/6/20
 */
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Company {
    private String name;
    private String address;
}
