package com.zfq.common.taskdistributor.swagger;

import java.util.List;

public interface ListGenerator<T> {
    List<T> generateList(String param);
}
