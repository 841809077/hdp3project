package com.hdp.project.hdpproject.kafka;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Liuyongzhi
 * @description:
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
