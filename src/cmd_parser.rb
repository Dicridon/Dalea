require 'pareater'
code = Pareater.new('./Dalea::CmdParser', '.cpp', '.hpp')
code.build do
  value "pool_file, p"
  value "warm_file, w"
  value "run_file, r"
  value "threads, t"
  value "batch, b"
end
code.generate!