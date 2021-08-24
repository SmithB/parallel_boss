function pworker

this_PID=num2str(feature('getPID'));
[~, out]=unix('tty'); this_tty=deblank(out);
[~, out]=unix('hostname'); temp=regexp(out,'^(\S+)\.\S+','tokens'); if ~isempty(temp); this_host=temp{1}{1};else this_host=deblank(out); end

comms.dir=sprintf('par_run/comms/worker_%s.%s', this_host, this_PID);

if ~exist('par_run','dir') || ~exist('par_run/comms','dir') ||  ~exist('par_run/running','dir')||  ~exist('par_run/done','dir') 
    error('expected directory structure does not exist')
end
comms.from_boss=sprintf('%s/to_worker/request.txt', comms.dir);
comms.to_boss=sprintf('%s/to_boss/request.txt', comms.dir);
comms.scratch=sprintf('%s/scratch.txt', comms.dir);

if exist(comms.dir,'dir') 
    error('comms_dir %s exists\n', comms.dir);
end
mkdir([comms.dir,'/to_worker'])
mkdir([comms.dir,'/to_boss'])

req_count=0; 
while exist(comms.dir,'dir')
    req_count=req_count+1;
    fid=fopen(comms.scratch,'w');
    fprintf(fid,'request new job %d;\n', req_count); 
    fclose(fid);
    movefile(comms.scratch, comms.to_boss);
   
    while ~exist(comms.from_boss,'file') && exist(comms.dir,'dir')
        pause(1);
    end
    if ~exist(comms.dir,'dir')
        break
    end
    
    fid=fopen(comms.from_boss,'r'); line=fgetl(fid); fclose(fid); delete(comms.from_boss);   
    temp=regexp(line, 'response\[(\d+)\] (.*);$','tokens');
    if isempty(temp) || str2double(temp{1}{1}) ~= req_count; 
        fprintf(1,'%s not understood', line); 
        continue
    end
    task_file=deblank(temp{1}{2});
    [~, task_filename]=fileparts(task_file);
    running_file=sprintf('par_run/running/%s-%s-%s.%d', task_filename, this_host, this_PID, req_count );
    done_file=sprintf('par_run/done/%s-%s-%s.%d', task_filename, this_host, this_PID, req_count );
    fid=fopen(task_file);
    this_cmd=fgetl(fid); 
    fclose(fid);
    delete(task_file);
    
    fprintf(1,'---------------------------------------\nTask:%d, from %s, got:\n %s\n', req_count, task_file, this_cmd);
    fprintf(1, datestr(now));
    fid=fopen(running_file,'w');
    fprintf(fid,'started: %s\n', datestr(now));
    fprintf(fid,'parent PID=%s\n', this_PID);
    fprintf(fid,'host=%s\n', this_host);
    fprintf(fid,'tty=%s\n', this_tty);
    fprintf(fid,'cmd:\n%s\n', this_cmd);
    fclose(fid);
    
    log_file=sprintf('par_run/logs/%s-%s-%s.%d.log', task_filename, this_host, this_PID, req_count );
    diary(log_file)
    eval(this_cmd);
    delete(running_file);
    diary('off')
    fid=fopen(done_file,'w');
    fprintf(fid,'started: %s\n', datestr(now));
    fprintf(fid,'PID=%s\n', this_PID);
    fprintf(fid,'tty=%s\n', this_tty);
    fprintf(fid,'host=%s\n', this_host);
    fprintf(fid,'cmd:\n%s\n', this_cmd);
    fprintf(fid,'finished: %s\n', datestr(now));
    fprintf(1,'finished: %s\n____________________\n', datestr(now));
    
    fclose(fid);   
end
    