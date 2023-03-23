package com.sat.demo.batchDBtoCSV.processor;

import com.sat.demo.batchDBtoCSV.entity.Customer;
import org.springframework.batch.item.ItemProcessor;

public class CustomerProcessor implements ItemProcessor<Customer,Customer> {
    @Override
    public Customer process(Customer customer) throws Exception {
        return customer;
    }
}
