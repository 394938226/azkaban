/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.executor.mail;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutionOptions.FailureAction;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.utils.EmailMessage;
import azkaban.utils.TimeUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.lang.exception.ExceptionUtils;

public class DefaultMailCreator implements MailCreator {

  public static final String DEFAULT_MAIL_CREATOR = "default";
  private static final HashMap<String, MailCreator> registeredCreators = new HashMap<>();
  private static final MailCreator defaultCreator;

  static {
    defaultCreator = new DefaultMailCreator();
    registerCreator(DEFAULT_MAIL_CREATOR, defaultCreator);
  }

  public static void registerCreator(final String name, final MailCreator creator) {
    registeredCreators.put(name, creator);
  }

  public static MailCreator getCreator(final String name) {
    MailCreator creator = registeredCreators.get(name);
    if (creator == null) {
      creator = defaultCreator;
    }
    return creator;
  }

  private static List<String> findFailedJobs(final ExecutableFlow flow) {
    final ArrayList<String> failedJobs = new ArrayList<>();
    for (final ExecutableNode node : flow.getExecutableNodes()) {
      if (node.getStatus() == Status.FAILED) {
        failedJobs.add(node.getId());
      }
    }
    return failedJobs;
  }

  @Override
  public boolean createFirstErrorMessage(final ExecutableFlow flow,
      final EmailMessage message, final String azkabanName, final String scheme,
      final String clientHostname, final String clientPortNumber) {

    final ExecutionOptions option = flow.getExecutionOptions();
    final List<String> emailList = option.getFailureEmails();
    final int execId = flow.getExecutionId();

    if (emailList != null && !emailList.isEmpty()) {
      message.addAllToAddress(emailList);
      message.setMimeType("text/html;charset=utf-8");
      message.setSubject("工作流 '" + flow.getFlowId() + "' 执行失败！ ");
      message.println("<h2 style=\"color:#FF0000\"> 项目'" + flow.getProjectName()+ "'的工作流'"+flow.getFlowId()+"' 任务'"+execId+"'执行失败！" +"</h2>");

      if (option.getFailureAction() == FailureAction.CANCEL_ALL) {
        message
            .println("工作流被设置为取消所有正在运行的作业");
      } else if (option.getFailureAction() == FailureAction.FINISH_ALL_POSSIBLE) {
        message
            .println("工作流被设置为完成所有作业并且忽略失败的作业");
      } else {
        message
            .println("工作流被设置为停止前，正确完成当前的所有作业");
      }

      message.println("<table>");
      message.println("<tr><td>开始时间：</td><td>"
          + TimeUtils.formatDateTimeZone(flow.getStartTime()) + "</td></tr>");
      message.println("<tr><td>结束时间：</td><td>"
          + TimeUtils.formatDateTimeZone(flow.getEndTime()) + "</td></tr>");
      message.println("<tr><td>执行耗时：</td><td>"
          + TimeUtils.formatDuration(flow.getStartTime(), flow.getEndTime())
          + "</td></tr>");
      message.println("<tr><td>状态：</td><td>" + flow.getStatus() + "</td></tr>");
      message.println("</table>");
      message.println("");

      return true;
    }

    return false;
  }

  @Override
  public boolean createErrorEmail(final ExecutableFlow flow, final List<ExecutableFlow>
      pastExecutions, final EmailMessage message, final String azkabanName, final String scheme,
      final String clientHostname, final String clientPortNumber, final String... reasons) {

    final ExecutionOptions option = flow.getExecutionOptions();

    final List<String> emailList = option.getFailureEmails();
    final int execId = flow.getExecutionId();

    if (emailList != null && !emailList.isEmpty()) {
      message.addAllToAddress(emailList);
      message.setMimeType("text/html;charset=utf-8");
      message.setSubject("工作流 '" + flow.getFlowId() + "' 执行失败！ ");
      message.println("<h2 style=\"color:#FF0000\"> 项目'" + flow.getProjectName()+ "'的工作流'"+flow.getFlowId()+"' 任务'"+execId+"'执行失败！" +"</h2>");
		  
      message.println("<table>");
      message.println("<tr><td>开始时间：</td><td>"
          + TimeUtils.formatDateTimeZone(flow.getStartTime()) + "</td></tr>");
      message.println("<tr><td>结束时间：</td><td>"
          + TimeUtils.formatDateTimeZone(flow.getEndTime()) + "</td></tr>");
      message.println("<tr><td>执行耗时：</td><td>"
          + TimeUtils.formatDuration(flow.getStartTime(), flow.getEndTime())
          + "</td></tr>");
      message.println("<tr><td>状态：</td><td>" + flow.getStatus() + "</td></tr>");
      message.println("</table>");
      message.println("");
      
      return true;
    }
    return false;
  }

  @Override
  public boolean createSuccessEmail(final ExecutableFlow flow, final EmailMessage message,
      final String azkabanName, final String scheme, final String clientHostname,
      final String clientPortNumber) {

    final ExecutionOptions option = flow.getExecutionOptions();
    final List<String> emailList = option.getSuccessEmails();

    final int execId = flow.getExecutionId();

    if (emailList != null && !emailList.isEmpty()) {
      message.addAllToAddress(emailList);
      message.setMimeType("text/html;charset=utf-8");
      message.setSubject("工作流 '" + flow.getFlowId() + "' 执行成功！ ");
      message.println("<h2 > 项目'" + flow.getProjectName()+ "'的工作流'"+flow.getFlowId()+"' 任务'"+execId+"'执行成功！" +"</h2>");
      message.println("<table>");
      message.println("<tr><td>开始时间：</td><td>"
          + TimeUtils.formatDateTimeZone(flow.getStartTime()) + "</td></tr>");
      message.println("<tr><td>结束时间：</td><td>"
          + TimeUtils.formatDateTimeZone(flow.getEndTime()) + "</td></tr>");
      message.println("<tr><td>执行耗时：</td><td>"
          + TimeUtils.formatDuration(flow.getStartTime(), flow.getEndTime())
          + "</td></tr>");
      message.println("<tr><td>状态：</td><td>" + flow.getStatus() + "</td></tr>");
      message.println("</table>");
      message.println("");
 
      return true;
    }
    return false;
  }

  @Override
  public boolean createFailedUpdateMessage(final List<ExecutableFlow> flows,
      final Executor executor, final ExecutorManagerException updateException,
      final EmailMessage message, final String azkabanName,
      final String scheme, final String clientHostname, final String clientPortNumber) {

    final ExecutionOptions option = flows.get(0).getExecutionOptions();
    final List<String> emailList = option.getFailureEmails();

    if (emailList != null && !emailList.isEmpty()) {
      message.addAllToAddress(emailList);
      message.setMimeType("text/html;charset=utf-8");
      message.setSubject("主机：" +executor.getHost() + "工作流状态更新失败！");

      message.println(
          "<h2 style=\"color:#FF0000\"> 工作流状态在主机： " + executor.getHost()
              + " 更新失败！" + "</h2>");

      message.println("这个节点上的所有被执行的作业，至少有一个作业状态更新失败！");

      message.println("");
      message.println("<h3>错误详情：</h3>");
      message.println("<pre>" + ExceptionUtils.getStackTrace(updateException) + "</pre>");

      message.println("");
      message.println("<h3>被影响的作业：</h3>");
      message.println("<ul>");
      for (final ExecutableFlow flow : flows) {
        final int execId = flow.getExecutionId();
 

//        message.println("<li>Execution '" + flow.getExecutionId() + "' of flow '" + flow.getFlowId()
//            + "' of project '" + flow.getProjectName() +
//            "</li>");
		
		message.println("<li>项目'" + flow.getProjectName() + "'的工作流 '" + flow.getFlowId()
            + "'任务'"  + flow.getExecutionId()+ "'</li>");
      }

      message.println("</ul>");
      return true;
    }

    return false;
  }
}

