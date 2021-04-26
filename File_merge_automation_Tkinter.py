
import tkinter as tk
import os
import getpass
from tkinter import messagebox

user_main_path = os.path.join("/Users", getpass.getuser())

# seacrh_files function
def search_files():
    print("Search files")
    print(string_text.get())
    string_input = string_text.get()
    if string_input == "" or string_input == " ":
        print("Enter a valid input")
        #messagebox.showerror("error", "Enter a valid file extension for search")
        #messagebox.showinfo("my message", "this is an example of showinfo\nmessagebox")
        messagebox.showwarning("warning", "show warning example in tkinter")
        return

    else:

        global all_file_names, all_file_paths
        all_file_names, all_file_paths = get_files_for_extension(user_main_path, string_text.get())

        files_list.delete(0, tk.END)
        for i in range(len(all_file_names)):
            files_list.insert(tk.END, all_file_paths[i])

        #for line in range(1, 26):
        #    files_list.insert(tk.END, "Geeks " + str(line))

def get_files_for_extension(path_name, extension_str):
    file_names_list = []
    file_paths_list = []

    for name in os.listdir(path_name):
        if name == "Library" or name == ".Trash" or name == "opt" or name.startswith(".") or name == "Pictures":
            continue
        if os.path.isdir(os.path.join(path_name, name)):
            temp_file_names_list, temp_files_path_list = get_files_for_extension(os.path.join(path_name, name),
                                                                                 extension_str)

            file_names_list += temp_file_names_list
            file_paths_list += temp_files_path_list

        elif os.path.isfile(os.path.join(path_name, name)):
            if name.endswith(extension_str):
                file_names_list.append(name)
                file_paths_list.append(os.path.join(path_name, name))

    return file_names_list, file_paths_list

# merge_files function
def merge_files():
    print("merge files")
    string_input = string_text.get()

    if not string_input == ".txt":
        print("Only .txt files can be merged for now")
        #messagebox.showerror("error", "Enter a valid file extension for search")
        #messagebox.showinfo("my message", "this is an example of showinfo\nmessagebox")
        messagebox.showwarning("warning", "show warning example in tkinter")
        return

    else:

        f = open("Merge" + string_input, "w")
        f.write("")
        f.close()

        f = open("Merge" + string_input, "a")
        for i in range(len(all_file_paths)):
            # print(file)
            f.write("(" + str(i + 1) + ") ")
            f.write(all_file_paths[i])

            f2 = open(all_file_paths[i], "r")
            f.write(f2.read())
            f2.close()

            f.write("\n\n\n\n")

        f.close()

#clear_input function
def clear_input():
    #print("clear input")
    files_list.delete(0, tk.END)
    string_entry.delete(0, tk.END)

# Create a window object
window = tk.Tk()

# String to be found
string_text = tk.StringVar()
string_label = tk.Label(window, text="String value", font = ('bold', 14), pady = 20, padx = 20)
string_label.grid(row=0, column=0, sticky="W")
string_entry = tk.Entry(window, textvariable = string_text)
string_entry.grid(row=0, column=1)


# Files list
files_list = tk.Listbox(window, height=8, width=60, border = 0)
files_list.grid(row = 2, column = 0, columnspan = 3, rowspan = 6, pady = 20, padx = (20,0))

# Create frame and Scroll bar
#my_frame = tk.Frame(window)
scroll_bar = tk.Scrollbar(window, orient=tk.VERTICAL)
scroll_bar.grid(row = 2, column=3, sticky = "nsw", rowspan=6, columnspan=3, pady = 20, padx = 0)

h_scroll_bar = tk.Scrollbar(window, orient=tk.HORIZONTAL)
h_scroll_bar.grid(row=6, column=0, sticky = "ew", columnspan = 3, rowspan = 6, pady = 20, padx = (20,0))

# Set scroll to list box
files_list.config(yscrollcommand=scroll_bar.set)
scroll_bar.config(command=files_list.yview)

files_list.config(xscrollcommand=h_scroll_bar.set)
h_scroll_bar.config(command=files_list.xview)

# Configure scrollbar
#scroll_bar.config(command=files_list.yview)
#scroll_bar.grid(row = 2, column=2)
#scroll_bar.pack(side=tk.RIGHT, fill=tk.Y)
#my_frame.pack()
#my_frame.grid(row = 0, column = 0)

# Buttons
search_button = tk.Button(window, text="Search Files", width = 12, command = search_files)
search_button.grid(row=1, column=0, pady=20)

merge_button = tk.Button(window, text="Merge all", width = 12, command = merge_files)
merge_button.grid(row=1, column=1, pady=20)

clear_button = tk.Button(window, text="Clear Input", width = 12, command = clear_input)
clear_button.grid(row=1, column=2, pady=20)

window.title("File search and merge - Automation")
window.wm_geometry('700x350')


# Start program
#window.withdraw()
window.mainloop()



