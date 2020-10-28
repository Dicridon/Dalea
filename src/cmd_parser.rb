require 'pareater'
code = Pareater.new('./Dalea::CmdParser', '.cpp', '.hpp')
code.build do
    value "pool_file, p"
    value "warm_file, w"
    value "run_file, r"
    optional "threads, t", default: "1"
end
code.generate!